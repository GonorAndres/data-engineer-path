---
tags: [index, navigation]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Data Engineering Knowledge Base

Welcome to the knowledge base. This is an Obsidian vault -- use the graph view to explore connections between topics.

## Learning Path

Follow this order. Each topic builds on the previous ones.

### Phase 1: Foundations
1. [[data-modeling-overview]] -- How to structure data for analytics (dimensional, 3NF, Data Vault)
2. [[sql-patterns]] -- Advanced SQL beyond SELECT: window functions, CTEs, recursive queries
3. [[etl-vs-elt]] -- The two paradigms and when each makes sense
4. [[data-warehouse-concepts]] -- What a warehouse is, why it exists, how it differs from a database

### Phase 2: Core Infrastructure
5. [[storage-layer]] -- Where data lives: object storage, databases, lakes, lakehouses
6. [[compute-layer]] -- How data gets processed: batch engines, stream engines, serverless
7. [[orchestration]] -- How data pipelines get scheduled and monitored
8. [[data-quality]] -- Testing, validation, and trust in your data

### Phase 3: GCP Deep Dives
9. [[bigquery-guide]] -- BigQuery architecture, optimization, and cost management
10. [[cloud-composer-guide]] -- Airflow on GCP: DAGs, operators, best practices
11. [[dataflow-guide]] -- Apache Beam on GCP: batch and streaming
12. [[gcs-as-data-lake]] -- Cloud Storage patterns for data engineering
13. [[pubsub-guide]] -- Real-time messaging on GCP
14. [[dataform-guide]] -- SQL transformation workflows (GCP's dbt alternative)

### Phase 4: Production Practices
15. [[ci-cd-for-data]] -- Testing and deploying data pipelines
16. [[infrastructure-as-code]] -- Terraform for data infrastructure
17. [[monitoring-observability]] -- Knowing when things break
18. [[data-governance]] -- Cataloging, lineage, access control

### Phase 5: Architecture
19. [[reference-architectures]] -- Common DE architecture patterns on GCP
20. [[cost-optimization]] -- How to not blow your cloud budget

## Decision Frameworks
- [[decisions/batch-vs-stream]]
- [[decisions/warehouse-selection]]
- [[decisions/orchestrator-selection]]
- [[decisions/storage-format-selection]]

## Projects
- See `../projects/` for hands-on portfolio work
