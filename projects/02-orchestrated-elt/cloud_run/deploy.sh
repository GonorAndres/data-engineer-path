#!/usr/bin/env bash
# =============================================================================
# deploy.sh -- Deploy Claims ELT Pipeline to Cloud Run + Cloud Scheduler
# =============================================================================
#
# This script deploys the pipeline as a Cloud Run service triggered by
# Cloud Scheduler on a daily cron.  This is the cost-effective alternative
# to Cloud Composer ($400+/month) for simple, linear DAGs.
#
# Cost estimates (as of March 2026, us-central1):
#   Cloud Run:
#     - Free tier: 2M requests/month, 360K vCPU-sec, 180K GiB-sec
#     - Pipeline runs ~3 min/day = ~90 min/month = 5,400 vCPU-sec
#     - Well within free tier.  If it exceeds: ~$0.05/month
#   Cloud Scheduler:
#     - Free tier: 3 jobs per account
#     - Beyond free tier: $0.10/job/month
#   Artifact Registry:
#     - Storage: $0.10/GB/month.  Image ~300 MB = ~$0.03/month
#     - Free egress within same region
#   Total: ~$0.00 - $0.18/month
#
#   For comparison:
#     Cloud Composer (smallest environment): ~$400/month
#     Self-managed Airflow on GCE: ~$30/month (e2-micro)
#     This approach: ~$0.10/month
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Docker installed (for local builds) or use Cloud Build
#   - GCP project with billing enabled
#   - Required APIs enabled (see below)
#
# Usage:
#   # Set your project ID
#   export GCP_PROJECT_ID=your-project-id
#
#   # Deploy everything
#   bash cloud_run/deploy.sh
#
#   # Or step by step:
#   bash cloud_run/deploy.sh setup      # Enable APIs, create resources
#   bash cloud_run/deploy.sh build      # Build and push container
#   bash cloud_run/deploy.sh deploy     # Deploy to Cloud Run
#   bash cloud_run/deploy.sh schedule   # Create Cloud Scheduler job
#   bash cloud_run/deploy.sh trigger    # Set up Eventarc GCS trigger
#
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID environment variable}"
REGION="${GCP_REGION:-us-central1}"
SERVICE_NAME="claims-elt-pipeline"
IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/data-pipelines/${SERVICE_NAME}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
SCHEDULER_JOB_NAME="claims-elt-daily"
# Service account for Cloud Run (least-privilege).
SA_NAME="claims-elt-runner"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
# GCS bucket for Eventarc trigger (optional -- for file-drop-triggered runs).
GCS_BUCKET="${GCS_BUCKET:-${PROJECT_ID}-claims-raw}"

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
info() { echo "[INFO] $*"; }
warn() { echo "[WARN] $*" >&2; }
error() { echo "[ERROR] $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Step 1: Setup -- enable APIs and create resources
# ---------------------------------------------------------------------------
setup() {
    info "Enabling required GCP APIs..."
    gcloud services enable \
        run.googleapis.com \
        artifactregistry.googleapis.com \
        cloudscheduler.googleapis.com \
        cloudbuild.googleapis.com \
        eventarc.googleapis.com \
        --project="${PROJECT_ID}"

    # Create Artifact Registry repository (if it doesn't exist).
    info "Creating Artifact Registry repository..."
    gcloud artifacts repositories create data-pipelines \
        --repository-format=docker \
        --location="${REGION}" \
        --description="Docker images for data pipeline services" \
        --project="${PROJECT_ID}" \
        2>/dev/null || info "Repository 'data-pipelines' already exists."

    # Create a dedicated service account for the Cloud Run service.
    # Principle of least privilege: this SA only has permissions it needs.
    info "Creating service account: ${SA_NAME}..."
    gcloud iam service-accounts create "${SA_NAME}" \
        --display-name="Claims ELT Pipeline Runner" \
        --description="Service account for the claims-elt-pipeline Cloud Run service" \
        --project="${PROJECT_ID}" \
        2>/dev/null || info "Service account '${SA_NAME}' already exists."

    # Grant minimum required roles to the service account.
    # - roles/bigquery.dataEditor: read/write BigQuery tables
    # - roles/bigquery.jobUser: run BigQuery jobs
    # - roles/storage.objectViewer: read CSV files from GCS
    # - roles/logging.logWriter: write structured logs to Cloud Logging
    info "Granting IAM roles to ${SA_EMAIL}..."
    for role in \
        roles/bigquery.dataEditor \
        roles/bigquery.jobUser \
        roles/storage.objectViewer \
        roles/logging.logWriter; do
        gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
            --member="serviceAccount:${SA_EMAIL}" \
            --role="${role}" \
            --condition=None \
            --quiet
    done

    info "Setup complete."
}

# ---------------------------------------------------------------------------
# Step 2: Build -- build and push the container image
# ---------------------------------------------------------------------------
build() {
    info "Building container image: ${IMAGE_NAME}:${IMAGE_TAG}..."

    # Option A: Build locally and push (faster iteration).
    # Requires Docker installed and authenticated to Artifact Registry.
    # gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

    # Option B: Use Cloud Build (no local Docker needed, ~$0.003/build).
    # Cloud Build runs in GCP and pushes directly to Artifact Registry.
    gcloud builds submit \
        --tag="${IMAGE_NAME}:${IMAGE_TAG}" \
        --project="${PROJECT_ID}" \
        --region="${REGION}" \
        --timeout=600s \
        .

    info "Image pushed: ${IMAGE_NAME}:${IMAGE_TAG}"
}

# ---------------------------------------------------------------------------
# Step 3: Deploy -- deploy to Cloud Run
# ---------------------------------------------------------------------------
deploy() {
    info "Deploying to Cloud Run: ${SERVICE_NAME}..."

    gcloud run deploy "${SERVICE_NAME}" \
        --image="${IMAGE_NAME}:${IMAGE_TAG}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --platform=managed \
        --service-account="${SA_EMAIL}" \
        \
        --command="python" \
        --args="cloud_run/entrypoint.py" \
        \
        --memory=1Gi \
        --cpu=1 \
        --timeout=900 \
        --max-instances=1 \
        --min-instances=0 \
        \
        --no-allow-unauthenticated \
        \
        --set-env-vars="PIPELINE_ENV=production,LOG_LEVEL=INFO" \
        \
        --labels="app=claims-elt,team=data-engineering,cost-center=analytics"

    # Get the service URL for scheduler configuration.
    SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
        --region="${REGION}" \
        --project="${PROJECT_ID}" \
        --format="value(status.url)")

    info "Service deployed: ${SERVICE_URL}"
    echo "${SERVICE_URL}" > /tmp/claims-elt-service-url.txt
}

# ---------------------------------------------------------------------------
# Step 4: Schedule -- create Cloud Scheduler job
# ---------------------------------------------------------------------------
schedule() {
    # Read service URL from deploy step or construct it.
    if [ -f /tmp/claims-elt-service-url.txt ]; then
        SERVICE_URL=$(cat /tmp/claims-elt-service-url.txt)
    else
        SERVICE_URL=$(gcloud run services describe "${SERVICE_NAME}" \
            --region="${REGION}" \
            --project="${PROJECT_ID}" \
            --format="value(status.url)")
    fi

    info "Creating Cloud Scheduler job: ${SCHEDULER_JOB_NAME}..."
    info "Schedule: 06:00 UTC daily (midnight CST)"

    # Delete existing job if it exists (idempotent deploys).
    gcloud scheduler jobs delete "${SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --quiet 2>/dev/null || true

    # Create the scheduler job.
    # - OIDC token: Cloud Scheduler authenticates to Cloud Run using the
    #   service account's OIDC token.  Cloud Run verifies the token
    #   against its IAM policy.
    # - POST to /run: triggers the pipeline.
    # - Retry: up to 3 attempts with exponential backoff.
    gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --schedule="0 6 * * *" \
        --time-zone="UTC" \
        --uri="${SERVICE_URL}/run" \
        --http-method=POST \
        --headers="Content-Type=application/json" \
        --message-body='{"seed": 42}' \
        --oidc-service-account-email="${SA_EMAIL}" \
        --oidc-token-audience="${SERVICE_URL}" \
        --attempt-deadline=900s \
        --max-retry-attempts=3 \
        --min-backoff-duration=30s \
        --max-backoff-duration=300s \
        --description="Daily trigger for the claims ELT pipeline at 06:00 UTC"

    info "Scheduler job created: ${SCHEDULER_JOB_NAME}"
    info "Next run: $(gcloud scheduler jobs describe "${SCHEDULER_JOB_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --format="value(scheduleTime)" 2>/dev/null || echo 'check console')"
}

# ---------------------------------------------------------------------------
# Step 5: Eventarc trigger (optional) -- trigger on GCS file upload
# ---------------------------------------------------------------------------
trigger() {
    info "Setting up Eventarc trigger for GCS bucket: ${GCS_BUCKET}..."

    # This trigger fires when a new file is uploaded to the GCS bucket.
    # Use case: source systems drop CSV files, pipeline auto-triggers.
    # This is an alternative to (or in addition to) the scheduled trigger.
    gcloud eventarc triggers create claims-elt-gcs-trigger \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --destination-run-service="${SERVICE_NAME}" \
        --destination-run-region="${REGION}" \
        --destination-run-path="/run" \
        --event-filters="type=google.cloud.storage.object.v1.finalized" \
        --event-filters="bucket=${GCS_BUCKET}" \
        --service-account="${SA_EMAIL}" \
        2>/dev/null || warn "Eventarc trigger already exists or bucket not found."

    info "Eventarc trigger configured."
    info "New files in gs://${GCS_BUCKET}/ will trigger pipeline runs."
}

# ---------------------------------------------------------------------------
# Main -- dispatch to step functions
# ---------------------------------------------------------------------------
main() {
    local step="${1:-all}"

    case "${step}" in
        setup)    setup ;;
        build)    build ;;
        deploy)   deploy ;;
        schedule) schedule ;;
        trigger)  trigger ;;
        all)
            setup
            build
            deploy
            schedule
            info ""
            info "=== Deployment complete ==="
            info "Service:   ${SERVICE_NAME}"
            info "Schedule:  Daily at 06:00 UTC"
            info "Est. cost: ~\$0.10/month"
            info ""
            info "Optional: run 'bash cloud_run/deploy.sh trigger' to add GCS event trigger."
            ;;
        *)
            error "Unknown step: ${step}. Use: setup, build, deploy, schedule, trigger, or all"
            ;;
    esac
}

main "$@"
