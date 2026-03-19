# =============================================================================
# GCS Module
# =============================================================================
# Creates two buckets:
#   1. Data bucket -- raw uploads, pipeline exports, sample data
#   2. Terraform state bucket -- remote state for team collaboration
#
# Features:
#   - Uniform bucket-level access (no per-object ACLs)
#   - Lifecycle rules: auto-delete test/ prefix after 30 days
#   - Versioning on state bucket (recover from bad state pushes)
#
# Cost: $0.02/GB/month for Standard storage.
#        Lifecycle rules prevent cost accumulation from test data.
# =============================================================================

# -----------------------------------------------------------------------------
# Data Bucket -- raw uploads, pipeline outputs, exports
# -----------------------------------------------------------------------------

resource "google_storage_bucket" "data" {
  name     = "${var.env_prefix_hyphen}claims-data-${var.project_id}"
  project  = var.project_id
  location = var.region

  # Prevent public access
  uniform_bucket_level_access = true

  # Standard storage class (cheapest for frequently accessed data)
  storage_class = "STANDARD"

  labels = merge(var.common_labels, {
    purpose = "pipeline-data"
  })

  # Auto-delete objects under test/ after 30 days
  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["test/"]
      with_state     = "ANY"
    }
    action {
      type = "Delete"
    }
  }

  # Move old raw data to Nearline after 90 days (cost savings)
  lifecycle_rule {
    condition {
      age            = 90
      matches_prefix = ["raw/"]
      with_state     = "ANY"
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # Versioning: keep one previous version for accidental overwrites
  versioning {
    enabled = true
  }

  # Force destroy only in dev (allows terraform destroy to delete non-empty buckets)
  force_destroy = var.environment == "dev" ? true : false
}

# -----------------------------------------------------------------------------
# Terraform State Bucket
# -----------------------------------------------------------------------------
# This bucket stores Terraform state files. Versioning is critical here
# to recover from corrupted or bad state pushes.

resource "google_storage_bucket" "tf_state" {
  name     = "${var.env_prefix_hyphen}tf-state-${var.project_id}"
  project  = var.project_id
  location = var.region

  uniform_bucket_level_access = true
  storage_class               = "STANDARD"

  labels = merge(var.common_labels, {
    purpose = "terraform-state"
  })

  versioning {
    enabled = true
  }

  # Keep state versions for 365 days before cleanup
  lifecycle_rule {
    condition {
      num_newer_versions = 10
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  # Never force-destroy the state bucket
  force_destroy = false
}
