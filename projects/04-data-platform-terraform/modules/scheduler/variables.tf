# =============================================================================
# Cloud Scheduler Module Variables
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Cloud Scheduler job region"
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

variable "pipeline_service_url" {
  description = "URL of the Cloud Run pipeline service to trigger"
  type        = string
}

variable "pipeline_service_account_email" {
  description = "Service account email for OIDC authentication with Cloud Run"
  type        = string
}
