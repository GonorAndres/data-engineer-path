# =============================================================================
# Cloud Scheduler Module
# =============================================================================
# Creates a Cloud Scheduler job that triggers the ELT pipeline daily.
#
# Schedule: 06:00 UTC daily (midnight CST / 01:00 CDT)
# Auth: OIDC token targeting the Cloud Run service URL
# Retry: 2 retries with 5-minute backoff
#
# Cost: $0.10/job/month. First 3 jobs per account are free.
# =============================================================================

resource "google_cloud_scheduler_job" "pipeline_trigger" {
  name    = "${var.env_prefix_hyphen}claims-pipeline-daily"
  project = var.project_id
  region  = var.region

  description = "Triggers the claims ELT pipeline daily at 06:00 UTC [${var.environment}]"

  # Daily at 06:00 UTC (midnight CST, good for end-of-day batch processing)
  schedule  = "0 6 * * *"
  time_zone = "UTC"

  # Pause in dev to avoid accidental charges (enable manually when testing)
  paused = var.environment == "dev" ? true : false

  # Retry policy: 2 retries, 5 minutes between attempts
  retry_config {
    retry_count          = 2
    min_backoff_duration = "300s" # 5 minutes
    max_backoff_duration = "300s" # 5 minutes (fixed interval)
    max_retry_duration   = "0s"   # No overall retry deadline
    max_doublings        = 0      # No exponential backoff (fixed 5-min interval)
  }

  # HTTP target: POST to Cloud Run pipeline service
  http_target {
    http_method = "POST"
    uri         = "${var.pipeline_service_url}/run"

    headers = {
      "Content-Type" = "application/json"
    }

    body = base64encode(jsonencode({
      triggered_by = "cloud-scheduler"
      environment  = var.environment
    }))

    # OIDC authentication: Cloud Run requires an identity token
    oidc_token {
      service_account_email = var.pipeline_service_account_email
      audience              = var.pipeline_service_url
    }
  }

  # Attempt deadline: 5 minutes (matches Cloud Run timeout)
  attempt_deadline = "300s"
}
