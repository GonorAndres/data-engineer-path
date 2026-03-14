# =============================================================================
# IAM Module Variables
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

variable "github_repo" {
  description = "GitHub repository in 'owner/repo' format for Workload Identity Federation. Empty string disables WIF."
  type        = string
  default     = ""
}
