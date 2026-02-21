---
tags: [project, portfolio, ml-pipeline, vertex-ai, pricing]
status: planned
created: 2026-02-21
updated: 2026-02-21
---

# Project 03: Insurance Pricing ML Feature Pipeline

## What It Demonstrates

An end-to-end ML feature engineering pipeline for insurance pricing. Raw policy and claims data gets transformed into features for a pricing model, demonstrating the ETL side of data engineering that feeds data science.

**Skills demonstrated:**
- Feature engineering pipeline design
- Dataproc (Spark) or Dataflow for heavy transforms
- Feature store patterns
- BigQuery ML (BQML) for simple models
- Vertex AI integration for production models
- Pipeline orchestration with Dagster or Composer

## Tech Stack

| Component | Tool |
|-----------|------|
| Feature transforms | Dataproc (PySpark) or BigQuery SQL |
| Model training | BigQuery ML + Vertex AI |
| Orchestration | Dagster (local) / Cloud Composer (production) |
| Feature storage | BigQuery |
| Source data | Claims warehouse from Project 01 + external risk data |

## Prerequisites

Complete Projects 01 and 02. This project uses the warehouse as a source.

## Actuarial Relevance

This directly mirrors how pricing actuaries work:
- **Exposure data** --> policy-level features
- **Claims data** --> loss experience features
- **External data** --> enrichment (weather, demographics)
- **Model** --> GLM/GAM for pure premium estimation
