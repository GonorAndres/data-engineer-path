# =============================================================================
# GCS Module Outputs
# =============================================================================

output "data_bucket_name" {
  description = "Name of the data bucket"
  value       = google_storage_bucket.data.name
}

output "data_bucket_url" {
  description = "URL of the data bucket (gs://...)"
  value       = google_storage_bucket.data.url
}

output "state_bucket_name" {
  description = "Name of the Terraform state bucket"
  value       = google_storage_bucket.tf_state.name
}

output "state_bucket_url" {
  description = "URL of the Terraform state bucket (gs://...)"
  value       = google_storage_bucket.tf_state.url
}
