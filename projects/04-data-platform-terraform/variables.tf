# =============================================================================
# Root Variables
# =============================================================================

variable "project_id" {
  description = "GCP project ID where resources will be created"
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "Project ID must be 6-30 characters, start with a letter, and contain only lowercase letters, digits, and hyphens."
  }
}

variable "region" {
  description = "GCP region for resource deployment"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev or prod). Controls resource naming and lifecycle policies."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "Environment must be 'dev' or 'prod'."
  }
}

variable "billing_budget_amount" {
  description = "Monthly billing budget in USD. Alerts fire at 50%, 80%, and 100%."
  type        = number
  default     = 100
}

variable "github_repo" {
  description = "GitHub repository in 'owner/repo' format for Workload Identity Federation (CI/CD)"
  type        = string
  default     = ""
}

variable "container_image" {
  description = "Container image URI for Cloud Run services (e.g., us-central1-docker.pkg.dev/PROJECT/repo/image:tag)"
  type        = string
  default     = "us-docker.pkg.dev/cloudrun/container/hello"
  # Default is a placeholder. Override with your actual pipeline image after first build.
}

variable "pubsub_push_endpoint" {
  description = "HTTPS endpoint for Pub/Sub push subscription (typically the Cloud Run subscriber URL)"
  type        = string
  default     = ""
  # Leave empty on first deploy; set after Cloud Run subscriber is live.
}
