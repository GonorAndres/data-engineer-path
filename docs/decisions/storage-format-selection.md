---
tags: [decisions, storage, file-formats, parquet, avro]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Decision: Storage Format Selection

## Context

When storing data in a data lake (GCS, S3) or exchanging between systems, you must choose a file format. This decision affects query performance, storage cost, and interoperability.

## Options

### Parquet

**Columnar** format. The default for analytics.

| Aspect | Details |
|--------|---------|
| Layout | Columnar -- data stored by column, not by row |
| Compression | Excellent (Snappy, Zstd, Gzip). Often 5-10x smaller than CSV |
| Read pattern | Fast for reading specific columns (SELECT a, b FROM ...) |
| Write pattern | Slower writes (must organize by column) |
| Schema | Embedded in file, self-describing |
| Ecosystem | Universal: BigQuery, Spark, DuckDB, Polars, pandas, Athena |

**When to use:** Analytics, warehouse loading, any case where you read columns not rows. **This is the default choice.**

### Avro

**Row-based** format with schema evolution.

| Aspect | Details |
|--------|---------|
| Layout | Row-based -- entire rows stored together |
| Compression | Good, but less than Parquet for analytical reads |
| Schema evolution | Excellent -- add/remove fields without breaking readers |
| Ecosystem | Kafka, Spark, BigQuery, Hive |

**When to use:** Event streaming (Kafka messages), when schema changes frequently, when you need full-row reads.

### CSV / JSON

**Human-readable** but inefficient.

| Aspect | CSV | JSON |
|--------|-----|------|
| Compression | None (unless gzipped) | None (unless gzipped) |
| Schema | None embedded | Self-describing but verbose |
| Types | Everything is a string | Basic types (string, number, bool) |
| Use case | Data exchange, small files, exports | API responses, configs, nested data |

**When to use:** Interop with non-technical users (CSV), API data ingestion (JSON). Convert to Parquet as soon as possible in your pipeline.

### Delta Lake / Iceberg / Hudi

**Table formats** that add ACID transactions on top of Parquet files.

| Format | Backed By | Key Feature |
|--------|-----------|-------------|
| Delta Lake | Databricks | Time travel, ACID, great Spark integration |
| Apache Iceberg | Netflix/Apple | Hidden partitioning, schema evolution, multi-engine |
| Apache Hudi | Uber | Incremental processing, record-level updates |

**When to use:** When you need a data lakehouse (transactions + lake storage). Iceberg is gaining the most momentum in 2025. BigQuery has native Iceberg support.

## Decision Framework

```
Is this for analytics / warehouse loading?
  YES -> Parquet (always)

Is this for event streaming (Kafka, Pub/Sub)?
  YES -> Avro (schema evolution matters)

Is this for data exchange with humans/external systems?
  YES -> CSV or JSON (convert to Parquet after ingestion)

Do you need ACID transactions on lake storage?
  YES -> Iceberg (broadest engine support) or Delta Lake (Databricks shops)

Is this for configuration or small structured data?
  YES -> JSON or YAML
```

## BigQuery-Specific Notes

- BigQuery natively reads: Parquet, Avro, CSV, JSON, ORC, Iceberg
- **Best for loading into BigQuery:** Parquet (columnar matches BQ's internal format)
- **External tables:** BigQuery can query Parquet files directly in GCS without loading
- **BigLake + Iceberg:** BigQuery can manage Iceberg tables in GCS for lakehouse pattern

## Related
- [[data-modeling-overview]] -- How format choice interacts with modeling
- [[bigquery-guide]] -- Loading data into BigQuery
- [[gcs-as-data-lake]] -- Storage layer where these files live
