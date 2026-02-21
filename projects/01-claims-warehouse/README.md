---
tags: [project, portfolio, claims, data-warehouse, bigquery]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Project 01: Insurance Claims Data Warehouse

## What It Demonstrates

Building a complete analytics warehouse for insurance claims data -- from raw ingestion to modeled facts and dimensions, with data quality checks and a BI-ready layer. This is the foundational DE project that touches every core concept.

**Skills demonstrated:**
- Data modeling (dimensional / star schema)
- SQL transformations (CTEs, window functions, MERGE)
- ELT pipeline design
- Data quality testing
- BigQuery optimization (partitioning, clustering)
- Loss triangle construction (actuarial domain expertise)

## Tech Stack

| Component | Tool | Why |
|-----------|------|-----|
| Warehouse | BigQuery (+ DuckDB for local dev) | Primary GCP focus |
| Transforms | Dataform or dbt | SQL-based ELT |
| Orchestration | Cloud Composer or Dagster | Pipeline scheduling |
| Data quality | dbt tests + Great Expectations | Validation layer |
| Storage | GCS (raw files) | Data lake for raw ingestion |
| Source data | Kaggle insurance datasets | Public, domain-relevant |

## Architecture

```
┌─────────────┐     ┌─────────┐     ┌──────────────────────────────────┐
│ Source Data  │────>│   GCS   │────>│           BigQuery               │
│ (CSV/JSON)  │     │ (raw/)  │     │                                  │
└─────────────┘     └─────────┘     │  stg_*  ──> int_*  ──> fct_*    │
                                    │  (raw)      (logic)    (final)   │
                                    │                         dim_*    │
                                    └──────────────────────────────────┘
                                                    │
                                              ┌─────┴──────┐
                                              │  Looker     │
                                              │  Studio     │
                                              └────────────┘
```

## Data Model

### Fact Tables
- `fct_claims` -- One row per claim event (incurred, paid, reserved amounts)
- `fct_claim_payments` -- One row per payment transaction on a claim

### Dimension Tables
- `dim_policyholder` -- Policyholder demographics and attributes
- `dim_policy` -- Policy details (coverage type, limits, deductibles)
- `dim_date` -- Standard date dimension
- `dim_coverage` -- Coverage type classifications

### Analytical Views
- `rpt_loss_triangle` -- Development triangle for reserving analysis
- `rpt_claim_frequency` -- Claim frequency by cohort

## How to Run

```bash
# Local development with DuckDB
cd projects/01-claims-warehouse
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Load sample data and run transforms locally
python src/main.py --target local

# Run data quality checks
python src/quality_checks.py

# Deploy to BigQuery (requires GCP project)
python src/main.py --target bigquery
```

## Project Phases

1. **Data exploration** -- Download and profile the source data
2. **Schema design** -- Define the dimensional model
3. **Staging layer** -- Clean and type-cast raw data
4. **Business logic** -- Build intermediate and fact/dim tables
5. **Quality checks** -- Implement data quality tests
6. **Loss triangle** -- Build actuarial-specific analytical views
7. **Dashboard** -- Connect to Looker Studio for visualization
8. **Documentation** -- Write up what was learned

## Related Docs
- [[data-modeling-overview]]
- [[sql-patterns]]
- [[bigquery-guide]]
- [[etl-vs-elt]]
