# =============================================================================
# Pub/Sub Module
# =============================================================================
# Creates the messaging infrastructure for streaming claims intake:
#   1. Claims events topic -- main topic for incoming claim events
#   2. Push subscription -- pushes to Cloud Run subscriber (real-time)
#   3. Pull subscription -- for Dataflow batch processing
#   4. Dead-letter topic -- captures failed messages for investigation
#   5. Dead-letter subscription -- pull sub on dead-letter for manual review
#
# Message retention: 7 days (allows replay of recent events).
#
# Cost: $0.04/GB after first 10 GB free tier.
#        At 1,000 claims/day (~1 KB each), monthly cost is ~$0.
# =============================================================================

# -----------------------------------------------------------------------------
# Claims Events Topic -- main ingest point for claim events
# -----------------------------------------------------------------------------

resource "google_pubsub_topic" "claims_events" {
  name    = "${var.env_prefix_hyphen}claims-events"
  project = var.project_id

  labels = merge(var.common_labels, {
    purpose = "claims-intake"
  })

  # Retain messages for 7 days (allows replay)
  message_retention_duration = "604800s" # 7 days

  # Schema enforcement can be added later:
  # schema_settings {
  #   schema   = google_pubsub_schema.claims_event.id
  #   encoding = "JSON"
  # }
}

# -----------------------------------------------------------------------------
# Dead-Letter Topic -- captures messages that fail processing
# -----------------------------------------------------------------------------

resource "google_pubsub_topic" "dead_letter" {
  name    = "${var.env_prefix_hyphen}claims-events-dead-letter"
  project = var.project_id

  labels = merge(var.common_labels, {
    purpose = "dead-letter"
  })

  message_retention_duration = "604800s" # 7 days
}

# -----------------------------------------------------------------------------
# Push Subscription -- delivers to Cloud Run subscriber (real-time)
# -----------------------------------------------------------------------------
# Only created when a push endpoint is provided (after Cloud Run is deployed).

resource "google_pubsub_subscription" "push" {
  count = var.push_endpoint != "" ? 1 : 0

  name    = "${var.env_prefix_hyphen}claims-events-push"
  project = var.project_id
  topic   = google_pubsub_topic.claims_events.id

  labels = merge(var.common_labels, {
    delivery = "push"
  })

  # Acknowledge deadline: 60 seconds (Cloud Run has up to 60s to process)
  ack_deadline_seconds = 60

  # Retry policy
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s" # 10 minutes max between retries
  }

  # Push configuration
  push_config {
    push_endpoint = var.push_endpoint

    # OIDC authentication (Cloud Run requires authenticated requests)
    oidc_token {
      service_account_email = var.push_service_account_email
    }

    attributes = {
      x-goog-version = "v1"
    }
  }

  # Dead-letter policy: after 5 failed attempts, send to dead-letter topic
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  # Message retention: 7 days
  message_retention_duration = "604800s"

  # Keep acknowledged messages for 7 days (allows replay)
  retain_acked_messages = true

  # Expiration: never (subscription persists even without activity)
  expiration_policy {
    ttl = "" # Never expires
  }
}

# -----------------------------------------------------------------------------
# Pull Subscription -- for batch processing (Dataflow or manual)
# -----------------------------------------------------------------------------

resource "google_pubsub_subscription" "pull" {
  name    = "${var.env_prefix_hyphen}claims-events-pull"
  project = var.project_id
  topic   = google_pubsub_topic.claims_events.id

  labels = merge(var.common_labels, {
    delivery = "pull"
  })

  # Acknowledge deadline: 120 seconds (batch jobs may take longer)
  ack_deadline_seconds = 120

  # Retry policy
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  # Dead-letter policy
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  # Message retention: 7 days
  message_retention_duration = "604800s"
  retain_acked_messages      = true

  expiration_policy {
    ttl = "" # Never expires
  }
}

# -----------------------------------------------------------------------------
# Dead-Letter Subscription -- manual review of failed messages
# -----------------------------------------------------------------------------

resource "google_pubsub_subscription" "dead_letter" {
  name    = "${var.env_prefix_hyphen}claims-events-dead-letter-sub"
  project = var.project_id
  topic   = google_pubsub_topic.dead_letter.id

  labels = merge(var.common_labels, {
    purpose = "dead-letter-review"
  })

  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  retain_acked_messages      = true

  expiration_policy {
    ttl = "" # Never expires
  }
}
