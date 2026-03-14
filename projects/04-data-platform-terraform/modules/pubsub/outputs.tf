# =============================================================================
# Pub/Sub Module Outputs
# =============================================================================

output "claims_topic_name" {
  description = "Name of the claims events topic"
  value       = google_pubsub_topic.claims_events.name
}

output "claims_topic_id" {
  description = "Fully-qualified ID of the claims events topic"
  value       = google_pubsub_topic.claims_events.id
}

output "dead_letter_topic_name" {
  description = "Name of the dead-letter topic"
  value       = google_pubsub_topic.dead_letter.name
}

output "pull_subscription_name" {
  description = "Name of the pull subscription"
  value       = google_pubsub_subscription.pull.name
}

output "push_subscription_name" {
  description = "Name of the push subscription (empty if not created)"
  value       = length(google_pubsub_subscription.push) > 0 ? google_pubsub_subscription.push[0].name : ""
}

output "dead_letter_subscription_name" {
  description = "Name of the dead-letter subscription"
  value       = google_pubsub_subscription.dead_letter.name
}
