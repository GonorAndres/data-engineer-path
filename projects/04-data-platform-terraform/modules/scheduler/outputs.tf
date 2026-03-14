# =============================================================================
# Cloud Scheduler Module Outputs
# =============================================================================

output "job_name" {
  description = "Name of the Cloud Scheduler job"
  value       = google_cloud_scheduler_job.pipeline_trigger.name
}

output "job_schedule" {
  description = "Cron schedule of the job"
  value       = google_cloud_scheduler_job.pipeline_trigger.schedule
}

output "job_paused" {
  description = "Whether the scheduler job is paused"
  value       = google_cloud_scheduler_job.pipeline_trigger.paused
}
