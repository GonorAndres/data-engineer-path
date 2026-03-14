# =============================================================================
# Root Outputs
# =============================================================================
# These outputs are displayed after `terraform apply` and can be referenced
# by other Terraform configurations or CI/CD scripts.

# -----------------------------------------------------------------------------
# BigQuery
# -----------------------------------------------------------------------------

output "bigquery_dataset_ids" {
  description = "Map of logical name to fully-qualified BigQuery dataset ID"
  value       = module.bigquery.dataset_ids
}

# -----------------------------------------------------------------------------
# GCS
# -----------------------------------------------------------------------------

output "gcs_data_bucket_name" {
  description = "Name of the GCS bucket for pipeline data (raw uploads, exports)"
  value       = module.gcs.data_bucket_name
}

output "gcs_state_bucket_name" {
  description = "Name of the GCS bucket for Terraform state"
  value       = module.gcs.state_bucket_name
}

# -----------------------------------------------------------------------------
# Pub/Sub
# -----------------------------------------------------------------------------

output "pubsub_claims_topic_name" {
  description = "Name of the claims events Pub/Sub topic"
  value       = module.pubsub.claims_topic_name
}

output "pubsub_dead_letter_topic_name" {
  description = "Name of the dead-letter Pub/Sub topic"
  value       = module.pubsub.dead_letter_topic_name
}

# -----------------------------------------------------------------------------
# Cloud Run
# -----------------------------------------------------------------------------

output "cloud_run_pipeline_url" {
  description = "URL of the Cloud Run ELT pipeline service"
  value       = module.cloud_run.pipeline_service_url
}

output "cloud_run_subscriber_url" {
  description = "URL of the Cloud Run Pub/Sub subscriber service"
  value       = module.cloud_run.subscriber_service_url
}

# -----------------------------------------------------------------------------
# IAM
# -----------------------------------------------------------------------------

output "pipeline_service_account_email" {
  description = "Email of the pipeline service account"
  value       = module.iam.pipeline_service_account_email
}
