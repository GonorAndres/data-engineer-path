---
tags: [decisions, orchestration]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Decision: Orchestrator Selection

## Context

You need to schedule, monitor, and manage data pipelines. Which orchestrator should you use?

## Quick Decision Matrix

| If you... | Choose... | Why |
|-----------|-----------|-----|
| Are on GCP and want managed | **Cloud Composer** (Airflow) | Native GCP integration, no infra management |
| Are building greenfield + care about lineage | **Dagster** | Asset-centric model, built-in quality tracking |
| Want fastest time-to-working-pipeline | **Prefect** | Decorate Python functions, done |
| Need maximum hiring pool | **Airflow** | Most common skill on resumes |
| Only orchestrate dbt runs | **dbt Cloud** or Airflow | dbt Cloud for pure SQL, Airflow for mixed |
| Have simple triggers (file arrives, cron) | **Cloud Workflows** + **Scheduler** | No need for a full orchestrator |

## Cost Comparison (GCP Context)

| Tool | Managed Option | Approx. Monthly Cost |
|------|---------------|---------------------|
| Cloud Composer 3 | Fully managed | ~$300-500/month minimum (small environment) |
| Dagster Cloud | SaaS | Free tier available, then usage-based |
| Prefect Cloud | SaaS | Free tier available, then usage-based |
| Self-hosted Airflow | You manage it | Infrastructure cost only, but ops overhead |

**Note:** Cloud Composer's minimum cost is significant for learning/small projects. For this knowledge base, start with Dagster or Prefect locally (free), learn the concepts, then use Composer when you have a GCP project with budget.

## Recommendation for This Project

1. **Learn with Dagster locally** -- free, excellent local dev, asset model teaches good habits
2. **Build production skills with Airflow/Composer** -- most marketable, what you'll see at work
3. **Know Prefect exists** -- useful for quick prototypes and small team scenarios

## Related
- [[orchestration]] -- Detailed comparison of all three tools
- [[cloud-composer-guide]] -- Cloud Composer deep dive
- [[dagster-local-guide]] -- Dagster for local development
- [[cost-effective-orchestration]] -- When Composer is overkill
