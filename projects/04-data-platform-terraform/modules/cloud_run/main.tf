# =============================================================================
# Cloud Run Module
# =============================================================================
# Deploys two Cloud Run services:
#   1. ELT Pipeline -- triggered by Cloud Scheduler (daily batch)
#   2. Pub/Sub Subscriber -- receives push messages from Pub/Sub (streaming)
#
# Cost controls:
#   - Max instances: 1 (prevents runaway scaling)
#   - Min instances: 0 (scale to zero when idle -- $0 at rest)
#   - Memory: 1 GiB, CPU: 1 (sufficient for SQL-based ELT)
#   - Request timeout: 300s (5 min, prevents hung containers)
#
# Security:
#   - No unauthenticated access (requires IAM or OIDC token)
#   - Runs as the pipeline service account (least privilege)
#
# Cost: $0 when idle. ~$0.01 per invocation for a 2-minute pipeline run.
#        CPU: $0.00002400/vCPU-second, Memory: $0.00000250/GiB-second
# =============================================================================

# -----------------------------------------------------------------------------
# ELT Pipeline Service -- daily batch execution
# -----------------------------------------------------------------------------

resource "google_cloud_run_v2_service" "pipeline" {
  name     = "${var.env_prefix_hyphen}claims-elt-pipeline"
  project  = var.project_id
  location = var.region

  # deletion_protection requires google provider >= 6.x; omit for 5.x compatibility

  template {
    # Service account for pipeline execution
    service_account = var.pipeline_service_account_email

    # Cost control: scale to zero, max 1 instance
    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }

    # Request timeout: 5 minutes (pipeline typically completes in ~2 min)
    timeout = "300s"

    containers {
      image = var.container_image

      # Resource limits -- sufficient for SQL-based ELT
      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
        # CPU only allocated during request processing (cost savings)
        cpu_idle = true
      }

      # Environment variables
      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      # Health check endpoint
      startup_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 0
        period_seconds        = 3
        failure_threshold     = 3
      }
    }

    labels = merge(var.common_labels, {
      service = "elt-pipeline"
    })
  }

  labels = merge(var.common_labels, {
    service = "elt-pipeline"
  })
}

# -----------------------------------------------------------------------------
# Pub/Sub Subscriber Service -- streaming claims intake
# -----------------------------------------------------------------------------

resource "google_cloud_run_v2_service" "subscriber" {
  name     = "${var.env_prefix_hyphen}claims-subscriber"
  project  = var.project_id
  location = var.region

  # deletion_protection requires google provider >= 6.x

  template {
    service_account = var.pipeline_service_account_email

    scaling {
      min_instance_count = 0
      max_instance_count = 1 # Cost control: single instance sufficient for low-volume
    }

    timeout = "60s" # Pub/Sub push messages have 60s deadline

    containers {
      image = var.container_image

      resources {
        limits = {
          cpu    = "1"
          memory = "1Gi"
        }
        cpu_idle = true
      }

      env {
        name  = "ENVIRONMENT"
        value = var.environment
      }

      env {
        name  = "GCP_PROJECT_ID"
        value = var.project_id
      }

      env {
        name  = "SERVICE_TYPE"
        value = "subscriber"
      }

      startup_probe {
        http_get {
          path = "/health"
        }
        initial_delay_seconds = 0
        period_seconds        = 3
        failure_threshold     = 3
      }
    }

    labels = merge(var.common_labels, {
      service = "subscriber"
    })
  }

  labels = merge(var.common_labels, {
    service = "subscriber"
  })
}

# -----------------------------------------------------------------------------
# IAM: No unauthenticated access
# -----------------------------------------------------------------------------
# By default, Cloud Run v2 services require authentication.
# We do NOT add an allUsers IAM binding, which means only authenticated
# callers (Cloud Scheduler via OIDC, Pub/Sub push, or gcloud) can invoke.
