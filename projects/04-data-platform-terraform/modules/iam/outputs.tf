# =============================================================================
# IAM Module Outputs
# =============================================================================

output "pipeline_service_account_email" {
  description = "Email of the pipeline service account"
  value       = google_service_account.pipeline.email
}

output "pipeline_service_account_id" {
  description = "Fully-qualified ID of the pipeline service account"
  value       = google_service_account.pipeline.id
}

output "pipeline_service_account_name" {
  description = "Name of the pipeline service account (for IAM bindings)"
  value       = google_service_account.pipeline.name
}

output "workload_identity_pool_name" {
  description = "Name of the Workload Identity Pool for GitHub Actions (empty if WIF disabled)"
  value       = length(google_iam_workload_identity_pool.github) > 0 ? google_iam_workload_identity_pool.github[0].name : ""
}

output "workload_identity_provider_name" {
  description = "Name of the Workload Identity Provider for GitHub Actions (empty if WIF disabled)"
  value       = length(google_iam_workload_identity_pool_provider.github) > 0 ? google_iam_workload_identity_pool_provider.github[0].name : ""
}
