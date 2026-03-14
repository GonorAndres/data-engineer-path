# =============================================================================
# Cloud Run Module Variables
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Cloud Run deployment region"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev or prod)"
  type        = string
}

variable "env_prefix_hyphen" {
  description = "Environment prefix with hyphen (e.g., 'dev-' or '')"
  type        = string
}

variable "common_labels" {
  description = "Labels applied to all resources"
  type        = map(string)
}

variable "pipeline_service_account_email" {
  description = "Email of the service account for Cloud Run execution"
  type        = string
}

variable "container_image" {
  description = "Container image URI (e.g., us-central1-docker.pkg.dev/project/repo/image:tag)"
  type        = string
}
