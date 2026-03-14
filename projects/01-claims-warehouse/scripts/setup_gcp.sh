#!/usr/bin/env bash
# GCP setup script for the Claims Warehouse project.
# Run this BEFORE any GCP spend to set up safety guardrails.
#
# Prerequisites:
#   gcloud auth login
#   gcloud config set project YOUR_PROJECT_ID
#
# Usage:
#   bash scripts/setup_gcp.sh YOUR_PROJECT_ID

set -euo pipefail

PROJECT_ID="${1:?Usage: bash scripts/setup_gcp.sh PROJECT_ID}"
REGION="us-central1"
BUCKET_NAME="${PROJECT_ID}-claims-data"

echo "=== Setting up GCP project: ${PROJECT_ID} ==="
gcloud config set project "${PROJECT_ID}"

# --- 1. Enable required APIs ---
echo ""
echo "--- Enabling APIs ---"
gcloud services enable \
    bigquery.googleapis.com \
    storage.googleapis.com \
    dataform.googleapis.com \
    cloudresourcemanager.googleapis.com \
    --quiet

echo "APIs enabled."

# --- 2. Create GCS bucket for raw data ---
echo ""
echo "--- Creating GCS bucket: ${BUCKET_NAME} ---"
if gsutil ls -b "gs://${BUCKET_NAME}" 2>/dev/null; then
    echo "Bucket already exists."
else
    gsutil mb -l "${REGION}" -p "${PROJECT_ID}" "gs://${BUCKET_NAME}"
    echo "Bucket created."
fi

# Lifecycle rule: auto-delete test data after 30 days
echo '{"rule": [{"action": {"type": "Delete"}, "condition": {"age": 30, "matchesPrefix": ["test/", "temp/"]}}]}' \
    | gsutil lifecycle set /dev/stdin "gs://${BUCKET_NAME}"
echo "Lifecycle rule set (auto-delete test/ and temp/ after 30 days)."

# --- 3. Upload sample data to GCS ---
echo ""
echo "--- Uploading sample data ---"
gsutil -m cp data/sample_data/*.csv "gs://${BUCKET_NAME}/raw/"
echo "Sample data uploaded to gs://${BUCKET_NAME}/raw/"

# --- 4. Create BigQuery datasets ---
echo ""
echo "--- Creating BigQuery datasets ---"
for DATASET in dev_claims_raw dev_claims_staging dev_claims_intermediate dev_claims_analytics dev_claims_reports dev_claims_assertions; do
    bq --project_id="${PROJECT_ID}" mk --dataset --location=US \
        --default_table_expiration 0 \
        --description "Claims warehouse: ${DATASET}" \
        "${PROJECT_ID}:${DATASET}" 2>/dev/null || echo "  ${DATASET} already exists"
done
echo "Datasets created."

# --- 5. Load raw CSVs into BigQuery ---
echo ""
echo "--- Loading raw data into BigQuery ---"
for CSV in policyholders policies claims claim_payments coverages; do
    bq load --autodetect --source_format=CSV \
        --max_bad_records=0 \
        "${PROJECT_ID}:dev_claims_raw.raw_${CSV}" \
        "gs://${BUCKET_NAME}/raw/${CSV}.csv"
    echo "  raw_${CSV} loaded"
done
echo "Raw data loaded."

# --- 6. Set up billing alerts ---
echo ""
echo "--- Billing alerts ---"
echo "IMPORTANT: Set up billing budget alerts manually in the GCP Console:"
echo "  1. Go to: https://console.cloud.google.com/billing/budgets"
echo "  2. Create budgets at: \$50, \$100, \$150, \$200, \$250"
echo "  3. Set alert thresholds at 50%, 90%, 100%"
echo "  4. Enable email notifications"
echo ""
echo "Also set BigQuery maximum_bytes_billed at the project level:"
echo "  bq update --default_table_expiration 0 --max_bytes_billed 10737418240 ${PROJECT_ID}"
echo ""

# --- 7. Summary ---
echo "=== Setup complete ==="
echo ""
echo "GCS bucket: gs://${BUCKET_NAME}"
echo "BigQuery datasets: dev_claims_raw, dev_claims_staging, dev_claims_intermediate, dev_claims_analytics, dev_claims_reports"
echo ""
echo "Next steps:"
echo "  1. Set up billing alerts (see instructions above)"
echo "  2. Create a Dataform repository in the BigQuery console"
echo "  3. Connect it to this repo's dataform/ directory"
echo "  4. Run the Dataform workflow"
