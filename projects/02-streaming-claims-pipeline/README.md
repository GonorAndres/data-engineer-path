---
tags: [project, portfolio, streaming, pubsub, dataflow]
status: planned
created: 2026-02-21
updated: 2026-02-21
---

# Project 02: Streaming Claims Intake Pipeline

## What It Demonstrates

Real-time ingestion of insurance claims events using GCP streaming services. Claims arrive via Pub/Sub, get processed by Dataflow, and land in BigQuery for near-real-time dashboarding.

**Skills demonstrated:**
- Pub/Sub messaging patterns
- Dataflow (Apache Beam) streaming pipelines
- Windowing, triggers, and watermarks
- Streaming inserts to BigQuery
- Event-driven architecture
- Error handling and dead-letter queues

## Tech Stack

| Component | Tool |
|-----------|------|
| Messaging | Pub/Sub |
| Processing | Dataflow (Apache Beam Python SDK) |
| Warehouse | BigQuery (streaming inserts) |
| Simulation | Python script generating fake claim events |
| Monitoring | Cloud Monitoring dashboards |

## Prerequisites

Complete Project 01 first. This project builds on the warehouse model from Project 01.

## Architecture

```
┌────────────┐    ┌──────────┐    ┌──────────────┐    ┌───────────┐
│ Claims     │──> │ Pub/Sub  │──> │  Dataflow    │──> │ BigQuery  │
│ Simulator  │    │ Topic    │    │  (Beam)      │    │ streaming │
└────────────┘    └──────────┘    │              │    └───────────┘
                                  │ - validate   │
                                  │ - enrich     │         │
                                  │ - window     │    ┌────┴────┐
                                  └──────┬───────┘    │ Looker  │
                                         │            │ Studio  │
                                  ┌──────┴───────┐    └─────────┘
                                  │ Dead Letter  │
                                  │ Queue (GCS)  │
                                  └──────────────┘
```
