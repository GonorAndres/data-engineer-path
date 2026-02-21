---
tags: [decisions, data-warehouse, bigquery, snowflake]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Decision: Data Warehouse Selection

## Context

Choosing a data warehouse is one of the most consequential decisions in a data platform. It affects cost, performance, team skills, and what tools integrate well. This doc compares the major options.

## Options

### BigQuery (GCP)

**Architecture:** Serverless, columnar, separation of storage and compute. You don't manage clusters -- Google does.

| Aspect | Details |
|--------|---------|
| Pricing | On-demand: $6.25/TB scanned. Flat-rate: slots (dedicated compute) |
| Storage | $0.02/GB/month (active), $0.01/GB/month (long-term after 90 days) |
| Scaling | Automatic, no cluster sizing decisions |
| SQL dialect | GoogleSQL (mostly ANSI, some extensions) |
| Strengths | Zero-ops, ML built-in (BQML), geo analytics, nested/repeated fields |
| Weaknesses | No transactions, limited update/delete patterns, vendor lock-in |

**Best for:** GCP-native shops, teams that want zero infrastructure management, analytics-heavy workloads.

### Snowflake

**Architecture:** Multi-cluster shared data. Virtual warehouses (compute) scale independently.

| Aspect | Details |
|--------|---------|
| Pricing | Per-credit compute + per-TB storage. Credits vary by cloud/region |
| Scaling | Manual or auto-scaling warehouse sizes |
| SQL dialect | ANSI SQL with extensions |
| Strengths | Multi-cloud, time travel, data sharing, semi-structured support |
| Weaknesses | Cost can spike with poorly tuned warehouses, credits model is opaque |

**Best for:** Multi-cloud environments, heavy data sharing needs, teams familiar with traditional SQL.

### Redshift (AWS)

| Aspect | Details |
|--------|---------|
| Pricing | Provisioned clusters or Serverless (per-RPU-hour) |
| Strengths | Deep AWS integration, Redshift Spectrum for S3 queries |
| Weaknesses | Cluster management overhead (provisioned), less elastic than BQ |

**Best for:** AWS-native shops, existing Redshift investments.

### DuckDB (Local/Embedded)

| Aspect | Details |
|--------|---------|
| Pricing | Free and open source |
| Architecture | In-process analytical database (like SQLite for analytics) |
| Strengths | Zero infrastructure, incredibly fast for local work, reads Parquet/CSV directly |
| Weaknesses | Single machine only, no concurrent users, not a production warehouse |

**Best for:** Local development, prototyping, small datasets, CI/CD testing, laptop analytics.

### Databricks SQL Warehouse

| Aspect | Details |
|--------|---------|
| Pricing | DBU-based, varies by tier |
| Strengths | Unified with Spark/ML, Delta Lake native, lakehouse architecture |
| Weaknesses | Complex pricing, heavy platform commitment |

**Best for:** Organizations already on Databricks for ML, lakehouse-first approach.

## Comparison Matrix

| Factor | BigQuery | Snowflake | Redshift | DuckDB | Databricks |
|--------|----------|-----------|----------|--------|------------|
| Ops overhead | None | Low | Medium | None | Low-Medium |
| Cost transparency | Good | Medium | Medium | Free | Low |
| Multi-cloud | No | Yes | No | Yes (local) | Yes |
| Streaming ingest | Yes | Yes (Snowpipe) | Yes | No | Yes |
| ML integration | BQML | Snowpark | SageMaker | No | Native |
| Semi-structured | Excellent (STRUCT, ARRAY) | Good (VARIANT) | Limited | Good | Good |
| Concurrency | Excellent | Good (with sizing) | Limited | Single user | Good |
| Ecosystem | GCP | Broad | AWS | Python/CLI | Broad |

## Decision Framework

```
Are you locked into a cloud provider?
  GCP -> BigQuery (default choice)
  AWS -> Redshift Serverless or Snowflake
  Azure -> Snowflake or Synapse
  Multi-cloud -> Snowflake

Is this for local development/prototyping?
  YES -> DuckDB (always, for local work)

Do you need lakehouse (unified analytics + ML)?
  YES -> Databricks or BigQuery + Vertex AI

Is cost predictability critical?
  YES -> BigQuery flat-rate or Snowflake with resource monitors
  "We prefer pay-per-query" -> BigQuery on-demand
```

## Recommendation for This Project

**BigQuery** as the primary warehouse (GCP-native, serverless, great for learning).
**DuckDB** as the local development companion (free, fast, reads the same Parquet files).

This combination lets you develop and test locally with DuckDB, then deploy to BigQuery for production -- same SQL concepts, different scale.

## Related
- [[bigquery-guide]] -- Deep dive on BigQuery architecture and optimization
- [[data-modeling-overview]] -- How modeling choices interact with warehouse capabilities
- [[decisions/storage-format-selection]] -- File formats that feed into the warehouse
