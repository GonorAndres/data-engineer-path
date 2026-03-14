---
tags: [fundamentals, orchestration, airflow, dagster, prefect]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Orchestration

Orchestration answers: **"When does each step of my data pipeline run, and what happens when something fails?"**

Without orchestration, you have a collection of scripts that someone runs manually or on a cron. With orchestration, you have a managed, observable, recoverable system.

## Core Concepts

| Concept | What It Means |
|---------|--------------|
| **DAG** | Directed Acyclic Graph -- your pipeline as a set of tasks with dependencies. Task B runs after Task A, never in a cycle. |
| **Task** | A single unit of work (run a query, call an API, move a file) |
| **Schedule** | When the DAG runs (cron expression, event-driven, manual) |
| **Backfill** | Re-running a pipeline for historical dates after fixing a bug or adding a new source |
| **Retry** | Automatic re-execution of a failed task (with configurable attempts and delay) |
| **Sensor** | A task that waits for an external condition (file appears, API returns success) |
| **Idempotency** | Running the same task twice produces the same result -- critical for reliability |

## The Big Three Orchestrators

### Apache Airflow / Cloud Composer (GCP)

The industry standard. Massive ecosystem, most job postings require it.

**Design philosophy:** Task-centric. You define "do this, then do that."

**Strengths:**
- Enormous community and provider ecosystem (1000+ operators)
- Airflow 3.0 (April 2025) brought significant improvements
- Cloud Composer = managed Airflow on GCP (no infrastructure to manage)
- Every DE team knows it, easy to hire for

**Weaknesses:**
- DAG authoring is Python-heavy but the DAG itself is configuration
- Testing DAGs locally is painful without a full Airflow environment
- UI can be slow for large deployments
- Task-centric view makes data lineage harder to track

**When to choose:** Default choice. Established teams, many integrations, GCP-native via Composer.

### Dagster

The modern challenger. Asset-centric design.

**Design philosophy:** Asset-centric. You define "what data should exist" and Dagster figures out what to run.

**Strengths:**
- First-class data lineage and asset tracking
- Excellent local development experience (runs on your laptop easily)
- Strong integration with dbt
- Built-in data quality checks tied to assets
- Components framework (GA October 2025) for reusable pipeline parts

**Weaknesses:**
- Smaller community than Airflow
- Fewer pre-built integrations
- Learning curve for the asset model if you're used to task-based thinking

**When to choose:** New projects, teams that prioritize data quality and lineage, heavy dbt usage.

### Prefect

Developer-friendly, cloud-native.

**Design philosophy:** Pythonic. Your existing Python functions become workflows with decorators.

**Strengths:**
- Lowest barrier to entry -- decorate your Python functions and you have a workflow
- Excellent error handling and retry logic
- Cloud-native with hybrid execution
- Dynamic flows (decisions at runtime, not just at definition time)

**Weaknesses:**
- Less mature ecosystem than Airflow
- Pricing can be complex at scale
- Less emphasis on data lineage compared to Dagster

**When to choose:** Small teams, rapid prototyping, teams that want minimal overhead.

## Comparison Table

| Factor | Airflow/Composer | Dagster | Prefect |
|--------|-----------------|---------|---------|
| Philosophy | Task-centric | Asset-centric | Function-centric |
| GCP managed | Cloud Composer | Dagster Cloud | Prefect Cloud |
| Local dev | Hard | Easy | Easy |
| Data lineage | Limited | Excellent | Limited |
| Community size | Massive | Growing | Growing |
| Learning curve | Medium | Medium-High | Low |
| dbt integration | Good | Excellent | Good |
| Job postings | Most common | Growing | Growing |

## Other Orchestration Tools

| Tool | Use Case |
|------|----------|
| **Cloud Workflows** (GCP) | Lightweight, serverless orchestration for API calls and GCP services. Not for data pipelines. |
| **Cloud Scheduler** (GCP) | Cron-as-a-service. Triggers Cloud Functions, HTTP endpoints. For single tasks, not DAGs. |
| **Mage** | Newer tool, notebook-style pipeline building. Good for experimentation. |
| **dbt Cloud** | Orchestrates dbt runs specifically. Not a general orchestrator. |

## Decision Framework

```
Are you building a new project from scratch?
  YES ->
    Is data lineage and quality a top priority?
      YES -> Dagster
      NO  ->
        Do you want minimal setup and your team thinks in Python?
          YES -> Prefect
          NO  -> Airflow (safest default, most hirable skill)
  NO (migrating or extending existing) ->
    Already using Airflow? -> Stay with Airflow
    Already on GCP? -> Cloud Composer (managed Airflow)
```

## Key Design Principle: Idempotency

Every task in your DAG should be **idempotent** -- running it twice with the same inputs produces the same output. This means:

- Use `MERGE` or `INSERT OVERWRITE` instead of `INSERT` (prevents duplicates)
- Partition by date and replace entire partitions, don't append
- Make API calls with idempotency keys where possible
- Never rely on "this task ran at exactly 3:00 AM" -- assume it might re-run

This is the #1 reliability principle in data engineering. If a task fails and you retry it, nothing should break.

## Related
- [[cloud-composer-guide]] -- Deep dive on managed Airflow on GCP
- [[dagster-local-guide]] -- Dagster for local development orchestration
- [[cost-effective-orchestration]] -- Cloud Scheduler + Cloud Run as a cheap alternative to Composer
- [[etl-vs-elt]] -- Orchestration schedules both ETL and ELT workflows
- [[ci-cd-for-data]] -- Deploying and testing DAGs
- [[data-quality]] -- Integrating quality checks into orchestrated pipelines
