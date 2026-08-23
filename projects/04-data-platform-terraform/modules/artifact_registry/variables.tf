# =============================================================================
# Artifact Registry Module Variables
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Artifact Registry location (must match where Cloud Run pulls from)"
  type        = string
}

variable "repository_id" {
  description = <<-EOT
    Repository name. Deliberately NOT environment-prefixed: the CI workflow
    hardcodes `data-pipelines` in the image path for both Cloud Run services,
    and images are already separated by service name and git SHA. Changing this
    means changing .github/workflows/ci-cd.yml in the same commit.
  EOT
  type        = string
  default     = "data-pipelines"
}

variable "common_labels" {
  description = "Labels applied to all resources"
  type        = map(string)
}

variable "keep_tagged_versions" {
  description = "How many tagged image versions to retain before cleanup deletes the oldest"
  type        = number
  default     = 10
}
