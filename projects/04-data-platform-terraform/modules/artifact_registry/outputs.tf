# =============================================================================
# Artifact Registry Module Outputs
# =============================================================================

output "repository_id" {
  description = "Repository name"
  value       = google_artifact_registry_repository.images.repository_id
}

output "repository_url" {
  description = "Host path CI pushes images to, without a trailing image name"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}
