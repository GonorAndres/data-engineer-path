---
tags: [fundamentals, data-quality, testing]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Data Quality

Data quality is the discipline of ensuring your data is **accurate, complete, consistent, and timely**. Without it, every dashboard and model downstream is suspect. In actuarial work, bad data quality means wrong reserves, wrong pricing, and regulatory risk.

## Why It's Critical

"Garbage in, garbage out" is the oldest rule in data. But in practice, data quality failures are subtle:
- A source system silently drops records one day
- A column that was never NULL suddenly has NULLs
- Dates arrive in a new format after a vendor update
- Duplicate records inflate metrics

You need **automated checks** that catch these before they corrupt your warehouse.

## Types of Data Quality Checks

| Check Type | What It Validates | Example |
|------------|-------------------|---------|
| **Schema** | Columns exist, types match | `claim_amount` is FLOAT, not STRING |
| **Completeness** | No unexpected NULLs | `policy_id` is never NULL |
| **Uniqueness** | No duplicates | `claim_id` is unique in `fct_claims` |
| **Referential integrity** | Foreign keys exist | Every `policy_id` in claims exists in policies |
| **Range/distribution** | Values within expected bounds | `claim_amount` > 0 and < 10,000,000 |
| **Freshness** | Data is up to date | Latest record is within last 24 hours |
| **Volume** | Row counts are reasonable | Today's load is within 20% of yesterday's |
| **Custom business rules** | Domain-specific logic | `incurred >= paid` for every claim |

## Tool Comparison

### dbt Tests (built into dbt/Dataform)

**What it is:** SQL-based tests defined alongside your transformation models.

**Strengths:**
- Zero additional infrastructure -- tests live with your models
- Four built-in generic tests: `unique`, `not_null`, `accepted_values`, `relationships`
- Custom tests are just SQL queries that return failing rows
- Runs during the transform step (catches issues before data reaches analytics)

**Weaknesses:**
- Only tests data inside the warehouse (not raw/source data)
- Limited anomaly detection and statistical checks

**When to use:** Always, as a baseline. If you use dbt/Dataform for transforms, you already have this.

### Great Expectations

**What it is:** Python framework for defining and running "expectations" against data.

**Strengths:**
- Rich library of expectations (200+ built-in)
- Works at any point in the pipeline (raw files, databases, warehouse)
- Generates HTML data docs (visual test reports)
- Profiling: auto-generates expectations from your data

**Weaknesses:**
- Heavy Python configuration
- Steeper learning curve than dbt tests or Soda
- Can feel over-engineered for simple use cases

**When to use:** Rigorous validation of raw data at ingestion, or when you need detailed profiling reports.

### Soda

**What it is:** Declarative data quality tool using SodaCL (its own YAML-like language).

**Strengths:**
- Very low barrier to entry (simple YAML checks)
- Built-in anomaly detection
- Continuous monitoring with scheduled checks
- Integrates with many warehouses and orchestrators

**Weaknesses:**
- Smaller community than GE or dbt tests
- SodaCL is yet another DSL to learn

**When to use:** Continuous monitoring on production warehouses, when you want simplicity.

## Complementary Usage Pattern

```
Raw Data (files/APIs)
  |
  ├── Great Expectations: validate schema and completeness at ingestion
  |
  v
Staging Layer (warehouse)
  |
  ├── dbt tests: unique, not_null, referential integrity on stg_ tables
  |
  v
Business Logic Layer
  |
  ├── dbt tests: custom business rules on fct_ and dim_ tables
  |
  v
Production Analytics
  |
  ├── Soda: continuous monitoring, anomaly detection, freshness checks
  |
  v
Dashboard / Reporting
```

## Actuarial-Specific Quality Checks

| Check | Why |
|-------|-----|
| `incurred >= paid` | Incurred losses should always be >= paid (IBNR) |
| Development factors > 1.0 (usually) | Cumulative development should increase |
| No future accident dates | Claims shouldn't have accident dates in the future |
| Policy effective < expiration | Date logic validation |
| Loss ratios within historical bands | Detect data issues that skew reserving |
| Triangle completeness | Every accident year/development period cell has data |

## Related
- [[sql-patterns]] -- SQL for implementing quality checks
- [[orchestration]] -- Running quality checks as pipeline steps
- [[data-modeling-overview]] -- Quality checks tied to model expectations
