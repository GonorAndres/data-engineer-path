# =============================================================================
# Terraform Backend Configuration
# =============================================================================
# Stores Terraform state in a GCS bucket for team collaboration and CI/CD.
#
# IMPORTANT: Create this bucket BEFORE running `terraform init`:
#   gsutil mb -l us-central1 gs://YOUR_PROJECT_ID-tf-state
#   gsutil versioning set on gs://YOUR_PROJECT_ID-tf-state
#
# Or use the GCS module's state bucket after bootstrapping with local state:
#   1. First run: comment out this backend block, use local state
#   2. Apply to create the GCS state bucket
#   3. Uncomment this block and run `terraform init -migrate-state`

terraform {
  backend "gcs" {
    bucket = "dev-tf-state-project-ad7a5be2-a1c7-4510-82d"
    prefix = "data-platform/state"
  }
}
