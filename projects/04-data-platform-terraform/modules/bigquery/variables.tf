# =============================================================================
# BigQuery Module Variables
# =============================================================================

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "BigQuery dataset location (region or multi-region)"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev or prod)"
  type        = string
}

variable "env_prefix" {
  description = "Environment prefix for dataset names (e.g., 'dev_' or '')"
  type        = string
}

variable "common_labels" {
  description = "Labels applied to all resources"
  type        = map(string)
}

variable "pipeline_service_account_email" {
  description = "Email of the pipeline service account (granted WRITER access)"
  type        = string
}

variable "default_table_expiration_ms" {
  description = <<-EOT
    Default table lifetime in ms for every dataset, or null for no expiry.
    Leave null for any dataset a live service reads: expiry silently deletes
    each table that long after the run that created it.
  EOT
  type        = number
  default     = null
}
