---
tags: [deployment, gcp, terraform, devops]
status: complete
created: 2026-03-16
updated: 2026-03-16
---

# Deployment Log

Central deployment narrative for the insurance claims data platform.

## Deployment Context

| Field | Value |
|-------|-------|
| **Date** | 2026-03-16 |
| **GCP Project** | `project-ad7a5be2-a1c7-4510-82d` |
| **Region** | `us-central1` |
| **Environment** | dev (prefix-based naming) |
| **Data** | Synthetic (500 policyholders, 800 policies, 608 claims, 2654 payments) |

---

## Deployment Sequence

### 1. Terraform (P04)

24 resources created via `terraform apply`. Backend bootstrapped with local state, migrated to GCS after apply.

**Fix**: Removed `deletion_protection` from Cloud Run module. The `google_cloud_run_v2_service` resource's `deletion_protection` attribute was added in google provider v6.x, but our pinned version (~> 5.0) installed v5.45 which does not support it.

### 2. Claims Warehouse (P01)

CSVs uploaded to GCS, loaded into BigQuery. Dataform deployed via Python SDK (`deploy_dataform.py`). 16 tables + 16 assertions all SUCCEEDED.

**Fixes**:
- Dataform service agent needed `bigquery.jobUser` + `bigquery.dataEditor` roles
- `defaultLocation` mismatch: datasets are in `us-central1` (regional), but Dataform's compile config had `US` (multi-region). Changed to `us-central1`.
- `coverages.csv` autodetect failure: `bq load --autodetect` treated the header as data on this 6-row CSV. Reloaded with explicit schema.

### 3. Cloud Run ELT (P02)

Docker image built and pushed to Artifact Registry. Deployed to Cloud Run. Health check and manual pipeline run both succeeded.

**Fix**: The Dockerfile CMD is `python -m pipeline` (batch mode), but Cloud Run expects an HTTP server for health checks and startup probes. Overrode CMD with `python cloud_run/entrypoint.py` which starts the HTTP handler.

### 4. Streaming Intake (P03)

Created Dockerfile (P03 did not have one). Built, pushed, deployed subscriber to Cloud Run. Set up Pub/Sub push subscription. 129 streaming claims landed in BigQuery.

**Fixes**:
- Pub/Sub service agent needed `iam.serviceAccountTokenCreator` to generate OIDC tokens for push authentication
- Push subscription needed `--push-auth-service-account` to send authenticated requests to Cloud Run
- `streaming_claims` table had to be pre-created for streaming inserts (BigQuery streaming inserts fail with 404 if the target table does not exist)

### 5. Verification

22 tables across 5 datasets. Both Cloud Run services show status Ready. No new errors observed after the fixes above.

### 6. Terraform State Migration

Moved from local state to GCS backend: `gs://dev-tf-state-project-ad7a5be2-a1c7-4510-82d/data-platform/state`. Migration performed via `terraform init -migrate-state` after the state bucket was created by the GCS module.

---

## What Broke During Deployment

| Issue | Project | Root Cause | Fix |
|-------|---------|-----------|-----|
| `deletion_protection` unsupported | P04 | google provider v5.45 does not have this attribute | Removed from cloud_run module |
| Dataform 403 permission denied | P01 | Dataform service agent missing BQ roles | Granted `bigquery.jobUser` + `bigquery.dataEditor` |
| Dataform "dataset not found in location US" | P01 | Datasets in us-central1, `defaultLocation` was US | Changed compile config to use `us-central1` |
| `coverages.csv` wrong column names | P01 | `bq load --autodetect` treated header as data (6-row CSV) | Reloaded with explicit schema |
| Cloud Run startup probe failed | P02 | Dockerfile CMD runs pipeline (batch), not HTTP server | Override CMD to `cloud_run/entrypoint.py` |
| Pub/Sub push 401 unauthorized | P03 | Push subscription missing OIDC auth config | Added `--push-auth-service-account` |
| Pub/Sub 403 token creation | P03 | Pub/Sub service agent cannot mint OIDC tokens | Granted `iam.serviceAccountTokenCreator` |
| `streaming_claims` table 404 | P03 | BQ streaming inserts require pre-existing table | Created table with `bq mk` before first insert |

---

## Cost

- **Estimated**: <$5 total for synthetic deployment
- All resources scale to zero when idle
- BigQuery storage is within free tier (10 GB)
- Cloud Run charges only during invocation

---

## Deployed Resources

| Resource | Identifier |
|----------|-----------|
| Cloud Run ELT | https://dev-claims-elt-pipeline-451451662791.us-central1.run.app |
| Cloud Run Subscriber | https://dev-claims-subscriber-451451662791.us-central1.run.app |
| GCS Bucket | `dev-claims-data-project-ad7a5be2-a1c7-4510-82d` |
| BigQuery | 5 datasets (`dev_claims_raw`, `dev_claims_staging`, `dev_claims_intermediate`, `dev_claims_analytics`, `dev_claims_reports`), 22 tables total |
| Pub/Sub | `claims-events` topic + `claims-events-dlq` topic |
| Dataform | `claims-warehouse-dataform` repository |
| Terraform State | `gs://dev-tf-state-project-ad7a5be2-a1c7-4510-82d/data-platform/state` |

---

## Scale Deployment (Phase 3)

**Date**: 2026-03-16 (same day as initial deployment)

### What Changed

| Dimension | Synthetic (Phase 1) | Scale (Phase 3) | Multiplier |
|-----------|-------------------|-----------------|------------|
| Policyholders | 500 | 100,000 | 200x |
| Policies | 800 | 160,000 | 200x |
| Claims | 608 | 125,634 | 207x |
| Payments | 2,654 | 543,067 | 205x |
| BigQuery storage | 0.3 MB | 420 MB | 1,400x |
| Data format | CSV (288 KB) | Parquet (24 MB) | -- |
| Dataform execution | ~60s | ~140s | 2.3x |
| Streaming rate | 2 events/sec | 100 events/sec | 50x |

### P01: Generator Scaling

Added `--policyholders` and `--output-format parquet` CLI flags to the data generator. At 100K scale:
- Generation time: ~30 seconds (Faker + NumPy)
- Parquet files: 24 MB total (vs 288 KB CSV)
- BigQuery load: Parquet loads faster and preserves types (no autodetect issues)
- Dataform re-run: all 32 actions SUCCEEDED with zero code changes to SQLX

### P06: freMTPL2 Real Data

Integrated the French Motor Third-Party Liability dataset (678K policies, 26.4K claims). Results:
- **Gini coefficient: 0.27** on test set (real actuarial discrimination)
- **Top risk factors**: log_premium (4.72), exposure (-1.58), young drivers 18-25 (+0.33), elderly 65+ (+0.22)
- **A/E ratio: 1.23** (model slightly conservative -- reasonable for a first GLM)
- Adapter maps freMTPL2 columns to P01 schema so feature SQL runs unchanged

### P03: High-Rate Streaming

Switched from synchronous to async batch publishing. At 100 events/sec:
- **2,449 events published** in 30 seconds
- **2,443 received** in BigQuery (99.8% delivery)
- Cloud Run subscriber handled load on single instance
- No errors in Cloud Logging

### What Broke

Nothing -- the scale deployment was clean. All code changes were backward-compatible, all infrastructure remained unchanged. The only notable observation was that Parquet loading eliminates the CSV autodetect issues from Phase 1 (coverages.csv header problem doesn't exist with Parquet).

---

## Related Docs

- [[infrastructure-as-code]] -- Terraform fundamentals
- [[cost-optimization]] -- GCP cost management strategies
- [[monitoring-observability]] -- Observability patterns for deployed services
