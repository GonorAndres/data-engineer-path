---
tags: [tools, terraform, iac, gcp, infrastructure]
status: draft
created: 2026-03-14
updated: 2026-03-14
---

# Terraform for GCP Data Engineering

## What Terraform Is

Terraform is a declarative Infrastructure as Code (IaC) tool. You describe the desired state of your infrastructure in `.tf` files, and Terraform figures out what API calls to make to reach that state. It tracks what it has created in a **state file**, so it knows the difference between "create new" and "update existing."

**Key mental model**: Terraform is a diff engine for infrastructure. Like `git diff` shows code changes, `terraform plan` shows infrastructure changes before they happen.

## Why Data Engineers Need It

Without IaC, your data platform is a snowflake -- rebuilt from memory, broken by accidental clicks, impossible to replicate across environments. Terraform solves:

| Problem | Without Terraform | With Terraform |
|---------|------------------|----------------|
| Environment consistency | Dev and prod drift apart | Same modules, different variables |
| Disaster recovery | "I think the settings were..." | `terraform apply` rebuilds everything |
| Onboarding | 20-page setup guide | `make init && make apply` |
| Change tracking | "Who changed the dataset?" | Git blame on `.tf` files |
| Cost visibility | Surprise bills | Resources are documented in code with cost comments |

## GCP Provider Configuration

```hcl
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

provider "google" {
  project = var.project_id
  region  = var.region
}
```

**When to use `google-beta`**: For resources that are in preview/beta in GCP (e.g., Workload Identity Federation features, new Cloud Run v2 features). The beta provider has the same interface but supports newer resource types.

## State Management with GCS Backend

Terraform state is the mapping between your `.tf` files and real GCP resources. By default, state is stored locally in `terraform.tfstate`. For team work and CI/CD, use a GCS backend:

```hcl
terraform {
  backend "gcs" {
    bucket = "my-project-tf-state"
    prefix = "data-platform/state"
  }
}
```

### Setting up the state bucket

```bash
# Create the bucket (do this once, manually or via a bootstrap script)
gsutil mb -l us-central1 gs://my-project-tf-state
gsutil versioning set on gs://my-project-tf-state
```

**Why versioning matters**: If a `terraform apply` corrupts your state (rare but possible), versioning lets you recover the previous state file.

### State Locking

GCS backend provides automatic state locking. If two `terraform apply` commands run simultaneously, the second one will fail with a lock error instead of corrupting state.

## Module Structure

Modules are reusable packages of Terraform configuration. The standard pattern:

```
modules/
├── bigquery/
│   ├── main.tf          # Resource definitions
│   ├── variables.tf     # Input parameters
│   └── outputs.tf       # Values exposed to the caller
├── gcs/
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
└── iam/
    ├── main.tf
    ├── variables.tf
    └── outputs.tf
```

**Root module** composes child modules:

```hcl
module "bigquery" {
  source = "./modules/bigquery"

  project_id  = var.project_id
  environment = var.environment
  # ... other variables
}

module "gcs" {
  source = "./modules/gcs"

  project_id  = var.project_id
  environment = var.environment
}
```

### Module Design Principles

1. **One concern per module**: `modules/bigquery` only creates BigQuery resources
2. **Parameterize everything**: environment, project_id, region are always variables
3. **Output useful values**: dataset IDs, bucket names, service URLs that other modules need
4. **Document costs**: comment expected costs on expensive resources

## Environment Separation (dev/prod)

Use variable-driven prefixes rather than separate directories:

```hcl
locals {
  env_prefix = var.environment == "prod" ? "" : "${var.environment}_"
}

resource "google_bigquery_dataset" "claims_raw" {
  dataset_id = "${local.env_prefix}claims_raw"
  # dev: "dev_claims_raw", prod: "claims_raw"
}
```

This approach lets dev and prod coexist in the same GCP project (useful for personal projects and small teams). For larger teams, use separate projects per environment.

## Import Existing Resources

If you already created GCP resources manually (via Console or gcloud), you can bring them under Terraform management:

```bash
# Import an existing BigQuery dataset
terraform import google_bigquery_dataset.claims_raw projects/my-project/datasets/claims_raw

# Import an existing GCS bucket
terraform import google_storage_bucket.data my-project-claims-data

# Import an existing service account
terraform import google_service_account.pipeline projects/my-project/serviceAccounts/pipeline-sa@my-project.iam.gserviceaccount.com
```

After importing, run `terraform plan` to see if your `.tf` configuration matches the actual resource. Fix any differences before applying.

### Terraform 1.5+ Import Blocks

Starting with Terraform 1.5, you can declare imports in HCL:

```hcl
import {
  to = google_bigquery_dataset.claims_raw
  id = "projects/my-project/datasets/claims_raw"
}
```

This is better than the CLI command because it is code-reviewed and repeatable.

## Common GCP Resources for Data Engineers

| Resource | Terraform Type | Typical Cost |
|----------|---------------|-------------|
| BigQuery dataset | `google_bigquery_dataset` | $0 (empty); $0.02/GB stored |
| BigQuery table | `google_bigquery_table` | Included in dataset cost |
| GCS bucket | `google_storage_bucket` | $0.02/GB/month Standard |
| Pub/Sub topic | `google_pubsub_topic` | First 10 GB free; $0.04/GB |
| Pub/Sub subscription | `google_pubsub_subscription` | Included in topic cost |
| Cloud Run service | `google_cloud_run_v2_service` | $0 idle; ~$0.01/invocation |
| Cloud Scheduler job | `google_cloud_scheduler_job` | $0.10/job/month; first 3 free |
| Service account | `google_service_account` | Free |
| IAM binding | `google_project_iam_member` | Free |

## Cost of Terraform

**Terraform itself is free and open-source.** You pay only for the GCP resources it provisions. The state file storage in GCS costs fractions of a cent.

There is a paid product (Terraform Cloud / HCP Terraform) for team features like remote state, policy enforcement, and a web UI. For personal projects and small teams, the open-source CLI with a GCS backend is sufficient.

## Key Commands

```bash
terraform init       # Download providers, set up backend
terraform plan       # Preview changes (ALWAYS do this before apply)
terraform apply      # Create/update resources
terraform destroy    # Delete all managed resources
terraform fmt        # Format .tf files consistently
terraform validate   # Check syntax without accessing APIs
terraform state list # List all managed resources
terraform output     # Show output values
terraform import     # Bring existing resources under management
```

## Safety Practices

1. **Always run `plan` before `apply`** -- review what will be created, changed, or destroyed
2. **Use `prevent_destroy`** on critical resources (production datasets, state buckets)
3. **Enable versioning** on the state bucket
4. **Never commit `terraform.tfvars`** -- it may contain project IDs or sensitive values
5. **Use `.gitignore`** for `.terraform/`, `*.tfstate`, `*.tfstate.backup`, `.terraform.lock.hcl`
6. **Pin provider versions** with `~> 5.0` (allows patch updates, prevents breaking changes)

## Related Docs

- [[bigquery-guide]] -- BigQuery resource design patterns
- [[gcs-as-data-lake]] -- GCS bucket design and lifecycle rules
- [[pubsub-guide]] -- Pub/Sub messaging infrastructure
- [[platform-reference-architecture]] -- Full platform architecture using these Terraform modules
- [[cost-effective-orchestration]] -- Why Cloud Run over Composer
