---
tags: [fundamentals, etl, elt, pipelines]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# ETL vs ELT

Two paradigms for moving and transforming data. The order of the letters tells you the difference.

## The Core Distinction

| | ETL (Extract, Transform, Load) | ELT (Extract, Load, Transform) |
|---|---|---|
| **Where transforms happen** | Outside the warehouse (Spark, Python, Dataflow) | Inside the warehouse (SQL in BigQuery, dbt) |
| **When transforms happen** | Before data lands in the warehouse | After data is already in the warehouse |
| **Data in warehouse** | Only cleaned/transformed data | Raw + transformed data (both available) |

## When to Use ETL

Use ETL when:
- You need to **filter or redact sensitive data** before it enters the warehouse (PII, HIPAA)
- Transformations are **computationally heavy** and don't map to SQL (ML feature engineering, complex parsing)
- You're working with **streaming data** that needs real-time processing
- Source data is **extremely large** and you want to reduce what you store (cost control)

**GCP tools for ETL:** Dataflow (Apache Beam), Dataproc (Spark), Cloud Functions (lightweight)

**Actuarial example:** Stripping PII from claims data before loading to the analytics warehouse -- names, SSNs, addresses get hashed or removed in the transform step.

## When to Use ELT

Use ELT when:
- Your warehouse is **powerful enough** to handle transforms (BigQuery, Snowflake -- yes, almost always)
- You want to **keep raw data** available for future use cases you haven't thought of yet
- Transformations are **expressible in SQL** (aggregations, joins, window functions)
- You want **faster iteration** -- change a transform without rebuilding the pipeline
- Your team **thinks in SQL** more than Python

**GCP tools for ELT:** BigQuery SQL + Dataform, dbt, Scheduled Queries

**Actuarial example:** Load raw policy and claims data into BigQuery, then use dbt/Dataform to build loss triangles, calculate development factors, and create reserving views -- all in SQL.

## The Modern Default: ELT

For most analytics use cases in 2025+, **ELT is the default**. Here's why:

1. **Cloud warehouses are cheap and powerful** -- BigQuery charges per query, not per storage. Let it do the heavy lifting.
2. **Raw data is preserved** -- When the business asks a new question 6 months from now, you don't need to rebuild the pipeline.
3. **SQL is more accessible** -- More people on the team can contribute to transforms.
4. **Faster development cycles** -- Change a dbt model, run it, see results in minutes.

## Hybrid: The Real World

Most production systems use both:

```
Sources --> Extract --> Load (raw) --> Transform (SQL/dbt) --> Analytics
                |
                +--> Transform (Python/Beam) --> Load (processed) --> ML
```

- **ELT path** for analytics/reporting (most of your work)
- **ETL path** for ML features, complex processing, or data that needs pre-processing

## Tool Comparison for Transforms

| Tool | Type | Best for | GCP Native? |
|------|------|----------|-------------|
| **Dataform** | ELT | SQL transforms in BigQuery | Yes |
| **dbt** | ELT | SQL transforms, any warehouse | No (but works with BQ) |
| **Dataflow** | ETL | Streaming, heavy batch processing | Yes |
| **Dataproc** | ETL | Spark workloads, ML pipelines | Yes |
| **Cloud Functions** | ETL | Lightweight event-driven transforms | Yes |
| **BigQuery SQL** | ELT | Ad-hoc transforms, scheduled queries | Yes |

## Decision Criteria

```
Is the transformation expressible in SQL?
  YES -> ELT (Dataform/dbt in BigQuery)
  NO  ->
    Is it a streaming use case?
      YES -> ETL (Dataflow)
      NO  ->
        Does it need heavy compute (Spark-scale)?
          YES -> ETL (Dataproc or Dataflow batch)
          NO  -> ETL (Cloud Functions or lightweight Python)
```

## Related
- [[data-modeling-overview]] -- What structure the transformed data should take
- [[orchestration]] -- How to schedule and chain ETL/ELT steps
- [[bigquery-guide]] -- Understanding BigQuery's processing model for ELT
- [[dataflow-guide]] -- Deep dive on Dataflow for ETL workloads
