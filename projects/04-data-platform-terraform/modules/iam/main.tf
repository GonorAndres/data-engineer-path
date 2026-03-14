# =============================================================================
# IAM Module
# =============================================================================
# Creates service accounts and role bindings for the claims data platform.
#
# Service accounts:
#   1. claims-pipeline-sa -- used by Cloud Run, Cloud Scheduler, and Dataform
#      to execute pipeline operations with least-privilege permissions
#
# Roles granted:
#   - BigQuery Data Editor (read/write tables, run queries)
#   - GCS Object Admin (read/write/delete objects)
#   - Pub/Sub Editor (publish and consume messages)
#   - Cloud Run Invoker (trigger Cloud Run services)
#
# Workload Identity Federation:
#   - Allows GitHub Actions to authenticate as the pipeline SA without
#     long-lived JSON keys (more secure, no key rotation needed)
#
# Cost: IAM is free. Service accounts are free.
# =============================================================================

# -----------------------------------------------------------------------------
# Pipeline Service Account
# -----------------------------------------------------------------------------

resource "google_service_account" "pipeline" {
  account_id   = "${var.env_prefix_hyphen}claims-pipeline-sa"
  display_name = "Claims Pipeline Service Account [${var.environment}]"
  description  = "Used by Cloud Run, Scheduler, and Dataform for pipeline execution"
  project      = var.project_id
}

# -----------------------------------------------------------------------------
# IAM Role Bindings -- least privilege for pipeline operations
# -----------------------------------------------------------------------------

# BigQuery Data Editor: read/write tables, run queries
resource "google_project_iam_member" "bigquery_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# BigQuery Job User: required to run queries (separate from data access)
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# GCS Object Admin: read/write/delete objects in buckets
resource "google_project_iam_member" "gcs_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Pub/Sub Editor: publish messages and manage subscriptions
resource "google_project_iam_member" "pubsub_editor" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Cloud Run Invoker: allows Cloud Scheduler to trigger Cloud Run services
resource "google_project_iam_member" "cloud_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# Service Account Token Creator: allows the SA to generate OIDC tokens
# (needed for Cloud Scheduler -> Cloud Run authentication)
resource "google_project_iam_member" "token_creator" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# -----------------------------------------------------------------------------
# Workload Identity Federation -- keyless auth for GitHub Actions
# -----------------------------------------------------------------------------
# This allows GitHub Actions to impersonate the pipeline service account
# without storing a JSON key in GitHub Secrets. Uses OIDC federation.

resource "google_iam_workload_identity_pool" "github" {
  count = var.github_repo != "" ? 1 : 0

  provider                  = google-beta
  project                   = var.project_id
  workload_identity_pool_id = "${var.env_prefix_hyphen}github-pool"
  display_name              = "GitHub Actions Pool [${var.environment}]"
  description               = "Workload Identity Pool for GitHub Actions CI/CD"
}

resource "google_iam_workload_identity_pool_provider" "github" {
  count = var.github_repo != "" ? 1 : 0

  provider                           = google-beta
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github[0].workload_identity_pool_id
  workload_identity_pool_provider_id = "${var.env_prefix_hyphen}github-provider"
  display_name                       = "GitHub Actions Provider [${var.environment}]"

  # GitHub's OIDC provider
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  # Map GitHub token claims to Google attributes
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  # Only allow tokens from the specified repository
  attribute_condition = "assertion.repository == '${var.github_repo}'"
}

# Allow the GitHub Actions workload to impersonate the pipeline SA
resource "google_service_account_iam_member" "workload_identity_binding" {
  count = var.github_repo != "" ? 1 : 0

  service_account_id = google_service_account.pipeline.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github[0].name}/attribute.repository/${var.github_repo}"
}
