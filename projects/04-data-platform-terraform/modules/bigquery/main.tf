# =============================================================================
# BigQuery Module
# =============================================================================
# Creates the five datasets that form the claims data warehouse layers:
#   1. claims_raw         -- Raw data loaded from GCS (CSV, JSON)
#   2. claims_staging     -- Cleaned, typed, deduplicated (stg_*)
#   3. claims_intermediate -- Enriched, joined, computed (int_*)
#   4. claims_analytics   -- Star schema: facts + dimensions (fct_*, dim_*)
#   5. claims_reports     -- Analytical reports (rpt_loss_triangle, etc.)
#
# Environment behavior:
#   dev:  Prefixed with "dev_", tables expire after 30 days
#   prod: No prefix, no expiration, prevent_destroy enabled
#
# Cost: $0 for empty datasets. $0.02/GB/month for stored data.
#        First 10 GB of storage is free.
# =============================================================================

locals {
  # Dataset definitions: logical name -> description
  datasets = {
    claims_raw = {
      description = "Raw data loaded from GCS. Source-of-truth, append-only."
    }
    claims_staging = {
      description = "Cleaned and type-cast data. One stg_ table per source."
    }
    claims_intermediate = {
      description = "Enriched and joined data. Business logic applied here."
    }
    claims_analytics = {
      description = "Star schema: fact and dimension tables for analytics."
    }
    claims_reports = {
      description = "Analytical reports: loss triangles, frequency analysis."
    }
  }

  # Dev tables expire after 30 days to avoid cost accumulation
  default_table_expiration_ms = var.environment == "dev" ? 2592000000 : null # 30 days in ms
}

# -----------------------------------------------------------------------------
# Datasets
# -----------------------------------------------------------------------------

resource "google_bigquery_dataset" "datasets" {
  for_each = local.datasets

  dataset_id  = "${var.env_prefix}${each.key}"
  project     = var.project_id
  location    = var.region
  description = "${each.value.description} [${var.environment}]"

  # Dev: tables expire after 30 days. Prod: no expiration.
  default_table_expiration_ms = local.default_table_expiration_ms

  labels = merge(var.common_labels, {
    data_layer = each.key
  })

  # Grant the pipeline service account editor access
  access {
    role          = "WRITER"
    user_by_email = var.pipeline_service_account_email
  }

  # Retain the default owner access for the project
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  # Allow all project readers to query
  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  # NOTE on prevent_destroy:
  # Terraform does not support conditional lifecycle blocks. For production
  # safety, use one of these approaches:
  #   1. Separate Terraform workspace with prevent_destroy = true
  #   2. Use -target to exclude datasets from destroy operations
  #   3. Set deletion_protection on individual tables
  # The lifecycle block below is set to false for dev flexibility.
  # For production deployments, fork this module and set prevent_destroy = true.
  lifecycle {
    prevent_destroy = false
  }
}
