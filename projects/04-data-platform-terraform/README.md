---
tags: [project, portfolio, terraform, iac, gcp]
status: planned
created: 2026-02-21
updated: 2026-02-21
---

# Project 04: GCP Data Platform with Terraform

## What It Demonstrates

Infrastructure as Code for a complete data platform. Define all GCP resources (BigQuery datasets, GCS buckets, Pub/Sub topics, IAM roles, Composer environments) in Terraform. This is the capstone project that ties everything together.

**Skills demonstrated:**
- Terraform for GCP resources
- Infrastructure as Code best practices
- GCP IAM and security
- Environment management (dev/staging/prod)
- CI/CD for infrastructure (GitHub Actions + Terraform)
- Cost management and resource tagging

## Tech Stack

| Component | Tool |
|-----------|------|
| IaC | Terraform |
| CI/CD | GitHub Actions |
| Cloud | GCP |
| State | GCS backend for Terraform state |

## Prerequisites

Complete Projects 01-03. This project codifies the infrastructure they used.

## What Gets Terraformed

- BigQuery datasets and table schemas
- GCS buckets with lifecycle policies
- Pub/Sub topics and subscriptions
- Cloud Composer environment
- IAM roles and service accounts
- Cloud Monitoring alerts
- Networking (if needed)
