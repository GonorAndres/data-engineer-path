# =============================================================================
# Artifact Registry Module
# =============================================================================
# The Docker repository every CI build pushes to, for both Cloud Run services.
#
# Why this module exists: on 2026-08-23 this repository had been deleted, and
# nothing noticed until a merge needed it. Lint and tests went green, both image
# pushes failed with `Repository "data-pipelines" not found`, both deploy jobs
# skipped, and the live services kept serving their previous revisions -- so the
# merge silently shipped nothing. The repository had been created by
# `projects/02-orchestrated-elt/cloud_run/deploy.sh`, a manual script CI never
# calls, and was declared in no Terraform anywhere.
#
# Every other GCP resource in this platform is declared. This one now is too.
#
# Cost: $0.10/GB/month beyond the 0.5 GB free tier. The cleanup policy below
# keeps that bounded without hand-pruning.
# =============================================================================

resource "google_artifact_registry_repository" "images" {
  project       = var.project_id
  location      = var.region
  repository_id = var.repository_id
  format        = "DOCKER"
  description   = "Container images for the claims data platform (CI/CD pushes here)"

  labels = var.common_labels

  # Keep the most recent tagged versions, delete older ones. Without this, every
  # merge to main adds two images forever -- the repository only grows.
  cleanup_policies {
    id     = "keep-recent-tagged"
    action = "KEEP"

    most_recent_versions {
      keep_count = var.keep_tagged_versions
    }
  }

  # Untagged layers are orphans from overwritten `:latest` pushes. Nothing can
  # pull them by digest in this setup, so they are pure storage cost.
  cleanup_policies {
    id     = "delete-untagged"
    action = "DELETE"

    condition {
      tag_state  = "UNTAGGED"
      older_than = "604800s" # 7 days
    }
  }

  lifecycle {
    # Deleting this repository breaks every deploy in the platform and cannot be
    # undone -- the images are gone with it. Matches the posture used on the
    # production BigQuery datasets and GCS buckets elsewhere in this project.
    prevent_destroy = true
  }
}
