# =============================================================================
# Root Module: GCP Data Platform
# =============================================================================
# Composes all child modules to provision the complete insurance claims
# data platform. Resources are environment-prefixed (dev/prod) so both
# can coexist in a single GCP project during development.
#
# Usage:
#   terraform init
#   terraform plan -var-file="terraform.tfvars"
#   terraform apply -var-file="terraform.tfvars"
# =============================================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }
}

# -----------------------------------------------------------------------------
# Providers
# -----------------------------------------------------------------------------

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

# -----------------------------------------------------------------------------
# Local values
# -----------------------------------------------------------------------------

locals {
  # Prefix for resource names: "dev-" for dev, "" for prod
  env_prefix = var.environment == "prod" ? "" : "${var.environment}_"

  # Hyphenated prefix for resources that don't allow underscores (GCS, Cloud Run)
  env_prefix_hyphen = var.environment == "prod" ? "" : "${var.environment}-"

  # Common labels applied to all resources
  common_labels = {
    environment = var.environment
    project     = "claims-data-platform"
    managed_by  = "terraform"
  }
}

# -----------------------------------------------------------------------------
# IAM Module -- create service accounts first (other modules depend on them)
# -----------------------------------------------------------------------------

module "iam" {
  source = "./modules/iam"

  project_id        = var.project_id
  environment       = var.environment
  env_prefix_hyphen = local.env_prefix_hyphen
  common_labels     = local.common_labels

  # Workload Identity for GitHub Actions CI/CD
  github_repo = var.github_repo
}

# -----------------------------------------------------------------------------
# BigQuery Module -- datasets for the claims data warehouse
# Cost: $0 when empty; $0.02/GB/month for stored data; first 10 GB free
# -----------------------------------------------------------------------------

module "bigquery" {
  source = "./modules/bigquery"

  project_id    = var.project_id
  region        = var.region
  environment   = var.environment
  env_prefix    = local.env_prefix
  common_labels = local.common_labels

  pipeline_service_account_email = module.iam.pipeline_service_account_email
}

# -----------------------------------------------------------------------------
# GCS Module -- buckets for raw data and Terraform state
# Cost: $0.02/GB/month standard; lifecycle rules auto-delete test data
# -----------------------------------------------------------------------------

module "gcs" {
  source = "./modules/gcs"

  project_id        = var.project_id
  region            = var.region
  environment       = var.environment
  env_prefix_hyphen = local.env_prefix_hyphen
  common_labels     = local.common_labels
}

# -----------------------------------------------------------------------------
# Pub/Sub Module -- messaging for streaming claims intake
# Cost: $0.04/GB after first 10 GB free
# -----------------------------------------------------------------------------

module "pubsub" {
  source = "./modules/pubsub"

  project_id        = var.project_id
  environment       = var.environment
  env_prefix_hyphen = local.env_prefix_hyphen
  common_labels     = local.common_labels

  # Push subscription endpoint (Cloud Run URL, set after first deploy)
  push_endpoint              = var.pubsub_push_endpoint
  push_service_account_email = module.iam.pipeline_service_account_email
}

# -----------------------------------------------------------------------------
# Cloud Run Module -- serverless containers for pipeline execution
# Cost: $0 at rest (scale to zero); ~$0.01 per invocation
# -----------------------------------------------------------------------------

module "cloud_run" {
  source = "./modules/cloud_run"

  project_id        = var.project_id
  region            = var.region
  environment       = var.environment
  env_prefix_hyphen = local.env_prefix_hyphen
  common_labels     = local.common_labels

  pipeline_service_account_email = module.iam.pipeline_service_account_email
  container_image                = var.container_image
}

# -----------------------------------------------------------------------------
# Cloud Scheduler Module -- cron triggers for daily pipeline runs
# Cost: $0.10/job/month; first 3 jobs free
# -----------------------------------------------------------------------------

module "scheduler" {
  source = "./modules/scheduler"

  project_id        = var.project_id
  region            = var.region
  environment       = var.environment
  env_prefix_hyphen = local.env_prefix_hyphen

  pipeline_service_url           = module.cloud_run.pipeline_service_url
  pipeline_service_account_email = module.iam.pipeline_service_account_email
}
