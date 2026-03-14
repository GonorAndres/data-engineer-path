---
tags: [fundamentals, data-modeling]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Data Modeling Overview

Data modeling is deciding **how to structure your data** so it can be queried efficiently and understood by humans. It's the single most impactful decision in a data warehouse -- get it wrong and everything downstream is painful.

## Why It Matters

Without a model, you have a pile of tables with unclear relationships. Analysts can't self-serve, queries are slow, and nobody trusts the numbers. A good model means:
- Analysts write simple queries that return correct results
- The warehouse performs well without constant tuning
- New data sources can be integrated without breaking everything

## The Main Approaches

### Dimensional Modeling (Kimball)

**When to use:** You have a clear analytics use case. Business users need to slice and dice metrics by dimensions. This is the default for most analytics warehouses.

| Concept | What it is | Example |
|---------|-----------|---------|
| Fact table | Stores measurable events (metrics) | `fct_claims` -- one row per claim event |
| Dimension table | Stores descriptive attributes | `dim_policyholder` -- policyholder details |
| Star schema | Facts in the center, dimensions around it | Claims fact joined to policyholder, date, coverage dims |
| Snowflake schema | Dimensions normalized further | Coverage dim splits into coverage_type sub-dimension |

**Trade-offs:**
- (+) Simple to query, fast to aggregate, business users understand it
- (+) Excellent for BI tools (Looker, Tableau, Looker Studio)
- (-) Requires upfront design work -- you need to know your business process
- (-) Can lead to data duplication (denormalized dimensions)

**Actuarial context:** Your claims triangle is essentially a fact table (claim events) with date and development period dimensions.

### Third Normal Form (3NF / Inmon)

**When to use:** You need a single source of truth with minimal redundancy. Common in operational databases and enterprise data warehouses where data integrity is paramount.

**Trade-offs:**
- (+) No data redundancy, easier to maintain consistency
- (+) Flexible -- supports many query patterns without redesign
- (-) Complex queries requiring many JOINs
- (-) Slower for analytical aggregations

### Data Vault

**When to use:** You have many source systems changing frequently, need full auditability/history, or work in regulated industries (insurance, banking).

| Component | Purpose | Example |
|-----------|---------|---------|
| Hub | Business keys | `hub_policy` (policy_number) |
| Link | Relationships between hubs | `link_policy_claim` |
| Satellite | Descriptive attributes + history | `sat_policy_details` (effective_date, premium, coverage) |

**Trade-offs:**
- (+) Handles source system changes gracefully
- (+) Full historical tracking built-in (important for actuarial reserving)
- (+) Parallel loading -- teams can work independently
- (-) Complex to query directly -- needs a presentation layer on top
- (-) More tables to manage
- (-) Overkill for small teams

### One Big Table (OBT)

**When to use:** Small team, simple analytics, modern columnar warehouses (BigQuery) that handle wide tables efficiently.

**Trade-offs:**
- (+) Dead simple to query -- one table, no JOINs
- (+) Great for dashboards with a single subject area
- (-) Data duplication everywhere
- (-) Doesn't scale to complex analytics across multiple business processes
- (-) Updates are expensive

## Decision Framework

```
Do you have < 5 source tables and one business process?
  YES -> One Big Table or simple Star Schema
  NO  ->
    Do you need full audit history and have many changing sources?
      YES -> Data Vault (with dimensional presentation layer)
      NO  ->
        Are analysts writing their own queries?
          YES -> Dimensional (Kimball) -- star schema
          NO  -> 3NF if integrity matters, Dimensional if performance matters
```

## Key Naming Conventions

Following [[sql-patterns]] and dbt conventions:

| Layer | Prefix | Purpose | Example |
|-------|--------|---------|---------|
| Staging | `stg_` | 1:1 with source, cleaned | `stg_claims_raw` |
| Intermediate | `int_` | Business logic, joins | `int_claims_enriched` |
| Fact | `fct_` | Measurable events | `fct_claim_payments` |
| Dimension | `dim_` | Descriptive attributes | `dim_coverage_type` |

## Further Reading
- [[etl-vs-elt]] -- How data moves into these models
- [[bigquery-guide]] -- How BigQuery's architecture affects modeling choices
- [[decisions/storage-format-selection]] -- Parquet, Avro, and how format affects modeling
- [[loss-triangle-construction]] -- How the star schema enables actuarial loss triangles
- [[duckdb-local-dev]] -- Develop and test dimensional models locally with DuckDB
