# =============================================================================
# Pub/Sub Module Variables
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
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

variable "push_endpoint" {
  description = "HTTPS endpoint for push subscription (Cloud Run URL). Empty string disables push sub."
  type        = string
  default     = ""
}

variable "push_service_account_email" {
  description = "Service account email for OIDC auth on push subscription"
  type        = string
  default     = ""
}
