---
tags: [project, portfolio, claims, data-warehouse, bigquery, duckdb]
status: draft
created: 2026-02-21
updated: 2026-03-14
---

# Project 01: Insurance Claims Data Warehouse

A complete analytics warehouse for insurance claims data -- from synthetic data generation to modeled facts/dimensions with a loss triangle report. Built locally with DuckDB, designed for BigQuery deployment.

## What It Demonstrates

- **Dimensional modeling** -- star schema with fact and dimension tables
- **SQL transformations** -- layered ELT (staging -> intermediate -> marts -> reports)
- **Data quality testing** -- 52 pytest tests covering schema, relationships, and business rules
- **Loss triangle construction** -- actuarial reserving analysis (the portfolio differentiator)
- **Claim frequency analysis** -- frequency, severity, pure premium, and loss ratio by coverage type
- **Actuarial data generation** -- Poisson frequency, lognormal severity, development patterns
- **Cost-effective development** -- full pipeline runs locally at $0 before any cloud spend

## Tech Stack

| Component | Tool | Why |
|-----------|------|-----|
| Local warehouse | DuckDB | Fast, serverless, SQL-compatible with BigQuery |
| Data generation | Faker (es_MX) + NumPy | Realistic actuarial distributions, Mexican context |
| Transforms | Raw SQL (DuckDB-compatible) | Portable to Dataform SQLX with minimal changes |
| Testing | pytest | Schema validation + data quality + SQL correctness |
| Target warehouse | BigQuery (Week 3-4) | GCP-native, serverless, Dataform integration |

## Architecture

```
┌──────────────────┐     ┌──────────────────────────────────────────────────┐
│  Data Generator   │     │                   DuckDB                        │
│  (Faker + NumPy)  │────>│                                                │
│  5 CSV files      │     │  raw_*  ──> stg_*  ──> int_*  ──> fct_*/dim_* │
└──────────────────┘     │  (load)     (clean)    (join)     (model)      │
                         │                                     │          │
                         │                              rpt_loss_triangle │
                         │                              rpt_claim_freq    │
                         └──────────────────────────────────────────────────┘
```

## Data Model

### Fact Tables
- `fct_claims` -- One row per claim event (paid, reserved, incurred amounts)
- `fct_claim_payments` -- One row per payment transaction with development context

### Dimension Tables
- `dim_policyholder` -- Policyholder demographics (age, state, occupation)
- `dim_policy` -- Policy details (coverage type, premium, deductible, limits)
- `dim_date` -- Date spine (2019-2026) with fiscal calendar
- `dim_coverage` -- Coverage type reference data

### Analytical Reports
- `rpt_loss_triangle` -- Development triangle for reserving analysis (IBNR)
- `rpt_claim_frequency` -- Frequency, severity, pure premium, loss ratio by year/coverage

## How to Run

```bash
cd projects/01-claims-warehouse

# Set up environment
python3 -m venv .venv && source .venv/bin/activate
pip install duckdb faker numpy polars pyarrow pytest

# Run the full pipeline (generate data + transform)
cd src && python3 main.py

# Or run specific steps:
python3 main.py --generate-only       # Only generate sample CSVs
python3 main.py --transform-only      # Only run SQL on existing data
python3 main.py --export results/     # Export marts to CSV
python3 main.py --persist             # Save to data/claims_warehouse.duckdb

# Run tests
cd .. && python3 -m pytest tests/ -v
```

## Sample Output

### Loss Triangle (Cumulative Paid, MXN)

```
    AY          Dev 0          Dev 1          Dev 2          Dev 3          Dev 4          Dev 5
  2020        749,565     3,494,214     4,740,957     5,888,645     6,365,676     6,680,344
  2021      1,786,100     5,414,082     7,218,443     8,171,883     8,721,034
  2022      1,528,648     3,484,551     4,243,697     5,023,656
  2023        897,136     2,435,283     4,033,524
  2024        473,722     1,659,902
  2025        826,273
```

The staircase pattern shows how older accident years are more fully developed.
Empty cells in the lower-right represent future development (IBNR).

## Project Structure

```
01-claims-warehouse/
├── README.md
├── pyproject.toml
├── src/
│   ├── data_generator.py    # Synthetic data with actuarial distributions
│   └── main.py              # DuckDB pipeline orchestrator
├── sql/
│   ├── staging/             # 1:1 with source, type cleaning
│   │   ├── stg_policyholders.sql
│   │   ├── stg_policies.sql
│   │   ├── stg_claims.sql
│   │   ├── stg_claim_payments.sql
│   │   └── stg_coverages.sql
│   ├── intermediate/        # Business logic, joins, computed fields
│   │   ├── int_claims_enriched.sql
│   │   ├── int_claim_payments_cumulative.sql
│   │   └── int_policy_exposure.sql
│   ├── marts/               # Final dimensional model
│   │   ├── dim_date.sql
│   │   ├── dim_policyholder.sql
│   │   ├── dim_policy.sql
│   │   ├── dim_coverage.sql
│   │   ├── fct_claims.sql
│   │   └── fct_claim_payments.sql
│   └── reports/             # Analytical views
│       ├── rpt_loss_triangle.sql
│       └── rpt_claim_frequency.sql
├── data/
│   └── sample_data/         # Generated CSVs (~288 KB total)
│       ├── policyholders.csv
│       ├── policies.csv
│       ├── claims.csv
│       ├── claim_payments.csv
│       └── coverages.csv
└── tests/
    ├── conftest.py                # Shared fixtures (DuckDB pipeline)
    ├── test_data_generator.py     # 27 tests: schema, distributions, relationships
    └── test_sql_transforms.py     # 25 tests: transforms, quality, loss triangle
```

## Synthetic Data Details

The data generator uses actuarial distributions to create realistic insurance data:

| Parameter | Auto | Home | Health | Liability | Life |
|-----------|------|------|--------|-----------|------|
| Poisson lambda | 0.12 | 0.05 | 0.20 | 0.03 | 0.005 |
| Severity median (MXN) | ~36K | ~60K | ~22K | ~100K | ~440K |
| Dev pattern length | 5 yr | 6 yr | 4 yr | 7 yr | 2 yr |

All data uses Mexican context: es_MX names, Mexican state codes, MXN currency.

## Related Docs

- [[data-modeling-overview]] -- Dimensional modeling concepts
- [[sql-patterns]] -- CTE, window function, pivot patterns used here
- [[duckdb-local-dev]] -- DuckDB as a local development warehouse
- [[loss-triangle-construction]] -- How loss triangles work and why they matter
- [[data-quality]] -- Data quality testing approach
- [[bigquery-guide]] -- Target deployment platform
