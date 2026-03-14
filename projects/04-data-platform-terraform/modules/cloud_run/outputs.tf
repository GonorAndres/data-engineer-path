# =============================================================================
# Cloud Run Module Outputs
# =============================================================================

output "pipeline_service_url" {
  description = "URL of the ELT pipeline Cloud Run service"
  value       = google_cloud_run_v2_service.pipeline.uri
}

output "pipeline_service_name" {
  description = "Name of the ELT pipeline Cloud Run service"
  value       = google_cloud_run_v2_service.pipeline.name
}

output "subscriber_service_url" {
  description = "URL of the Pub/Sub subscriber Cloud Run service"
  value       = google_cloud_run_v2_service.subscriber.uri
}

output "subscriber_service_name" {
  description = "Name of the Pub/Sub subscriber Cloud Run service"
  value       = google_cloud_run_v2_service.subscriber.name
}
