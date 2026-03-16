---
tags: [decisions, architecture, streaming, batch]
status: complete
created: 2026-02-21
updated: 2026-03-15
---

# Decision: Batch vs Stream Processing

## Context

Every data pipeline must decide: process data in batches on a schedule, or process it continuously as it arrives?

This is NOT a binary choice. Most systems use both. The question is: **for this specific data flow, which approach?**

## Options

### Batch Processing

Process data in discrete chunks on a schedule (hourly, daily, etc.).

**GCP tools:** BigQuery scheduled queries, Dataform, Dataflow batch, Dataproc
**Open source:** dbt, Spark batch, Airflow-scheduled scripts

**Characteristics:**
- Higher latency (minutes to hours)
- Simpler to build, test, debug, and recover from failures
- Cost-efficient (process in bulk, shut down)
- Easier to reason about data completeness

### Stream Processing

Process data continuously as individual events or micro-batches arrive.

**GCP tools:** Dataflow streaming, Pub/Sub + Cloud Functions, BigQuery streaming inserts
**Open source:** Kafka + Flink, Kafka Streams, Spark Structured Streaming

**Characteristics:**
- Low latency (seconds to minutes)
- Complex to build and debug (ordering, late data, exactly-once)
- Always-on infrastructure = higher base cost
- Harder to handle failures and reprocessing

### Micro-Batch (Hybrid)

Process in small batches at very short intervals (every 1-5 minutes).

**GCP tools:** Dataflow with windowing, BigQuery streaming buffer
**Open source:** Spark Structured Streaming

**Characteristics:**
- Near-real-time latency with batch-like simplicity
- Good compromise for many use cases
- Often "good enough" when someone says they need streaming

## Trade-offs

| Factor | Batch | Stream | Micro-Batch |
|--------|-------|--------|-------------|
| Latency | Minutes to hours | Seconds | 1-5 minutes |
| Complexity | Low | High | Medium |
| Cost | Low (run and stop) | High (always on) | Medium |
| Failure recovery | Easy (rerun the batch) | Hard (state management) | Medium |
| Data completeness | Easy to guarantee | Hard (late-arriving data) | Medium |
| Testing | Standard unit/integration tests | Requires event simulation | Standard + timing tests |

## Decision Framework

```
What is the business requirement for data freshness?

  "We need it within seconds" (fraud detection, real-time pricing)
    -> Stream processing
    -> BUT: Challenge this requirement. Does it REALLY need to be seconds?

  "Within a few minutes is fine" (operational dashboards, alerting)
    -> Micro-batch (Dataflow with short windows, or frequent batch runs)

  "Hourly or daily is fine" (reporting, analytics, reserving)
    -> Batch processing (the vast majority of DE work)

  "It depends on the data"
    -> Lambda architecture: batch for completeness + stream for freshness
    -> Or: Kappa architecture: stream everything, but this is complex
```

## Actuarial Context

| Use Case | Approach | Why |
|----------|----------|-----|
| Monthly reserving reports | Batch (daily/monthly) | No urgency, need completeness |
| Claims intake dashboard | Micro-batch (5-min) | Ops team wants near-real-time view |
| Fraud detection on new claims | Stream | Must act before claim is paid |
| Pricing model refresh | Batch (weekly/monthly) | Models retrained on accumulated data |
| Catastrophe event monitoring | Stream | Need immediate exposure aggregation |

## Recommendation

**Default to batch.** Switch to streaming only when there's a clear, validated business need for low latency. The complexity and cost overhead of streaming is significant, and "we want real-time" from stakeholders usually means "we want it faster than monthly" -- which a daily or hourly batch solves.

## When to Revisit

- Business requirements change (new real-time use case)
- Data volume grows beyond what batch windows can handle
- Cost of streaming infrastructure drops significantly

## Related
- [[etl-vs-elt]] -- ETL is more common for streaming, ELT for batch
- [[dataflow-guide]] -- Dataflow handles both batch and stream
- [[pubsub-guide]] -- The messaging layer for streaming on GCP
- [[orchestration]] -- Scheduling batch processes
