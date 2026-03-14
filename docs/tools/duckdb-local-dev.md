---
tags: [tools, duckdb, local-development]
status: draft
created: 2026-03-14
updated: 2026-03-14
---

# DuckDB for Local Data Engineering Development

DuckDB is an in-process analytical database that runs entirely within your application -- no server, no setup, no cost. It's the best tool for developing and testing SQL transforms locally before deploying to BigQuery.

## Why DuckDB for DE Development

| Benefit | Details |
|---------|---------|
| Zero cost | No cloud spend during development |
| Fast iteration | Sub-second query times on GB-scale data |
| SQL compatibility | Very close to BigQuery's SQL dialect |
| File ingestion | Read CSV, Parquet, JSON directly with `read_csv_auto()` |
| Embedded | No server process -- runs in your Python script |
| Portable | Single file database, or fully in-memory |

## When to Use DuckDB vs BigQuery

```
Is this production data or does it need cloud access?
  YES -> BigQuery
  NO  ->
    Are you writing/testing SQL transforms?
      YES -> DuckDB locally, then port to BigQuery
    Are you exploring a dataset < 100 GB?
      YES -> DuckDB (faster than spinning up BQ jobs)
    Do you need BigQuery-specific features (QUALIFY, ML, BI Engine)?
      YES -> BigQuery sandbox (free tier: 1 TB/month queries)
```

## Key DuckDB SQL Patterns

### Loading Data

```sql
-- CSV (auto-detect schema)
CREATE TABLE raw_claims AS
SELECT * FROM read_csv_auto('data/sample_data/claims.csv');

-- Parquet
CREATE TABLE raw_policies AS
SELECT * FROM read_parquet('data/policies.parquet');

-- Multiple files with glob
CREATE TABLE all_claims AS
SELECT * FROM read_csv_auto('data/claims_*.csv');
```

### DuckDB <-> BigQuery SQL Differences

| Feature | DuckDB | BigQuery |
|---------|--------|----------|
| Date extraction | `EXTRACT(YEAR FROM d)` | `EXTRACT(YEAR FROM d)` |
| Date formatting | `STRFTIME(d, '%Y%m%d')` | `FORMAT_DATE('%Y%m%d', d)` |
| Date series | `generate_series(start, end, INTERVAL '1 day')` | `GENERATE_DATE_ARRAY(start, end, INTERVAL 1 DAY)` |
| Safe cast | `TRY_CAST(x AS INT)` | `SAFE_CAST(x AS INT64)` |
| Last day of month | `LAST_DAY(d)` | `LAST_DAY(d)` |
| QUALIFY clause | Supported (v0.9+) | Supported |
| Integer types | `INTEGER`, `BIGINT` | `INT64` |
| Decimal types | `DECIMAL(p,s)` | `NUMERIC` |

Most DuckDB SQL ports to BigQuery with minimal changes. Write in DuckDB first, then adapt.

### In the Claims Warehouse Project

The pipeline in [[projects/01-claims-warehouse]] uses DuckDB as follows:

1. `data_generator.py` writes CSVs to `data/sample_data/`
2. `main.py` loads CSVs into DuckDB with `read_csv_auto()`
3. SQL files in `sql/` execute in order: staging -> intermediate -> marts -> reports
4. All 16 tables are created in-memory (or persisted to `.duckdb` file)
5. Tests run the same pipeline against a fresh in-memory database

This means the full pipeline costs $0 and runs in under a second.

## Python Integration

```python
import duckdb

# In-memory (fastest, ephemeral)
con = duckdb.connect(":memory:")

# Persistent file
con = duckdb.connect("warehouse.duckdb")

# Execute SQL from file
sql = Path("sql/staging/stg_claims.sql").read_text()
con.execute(sql)

# Query to Python
df = con.execute("SELECT * FROM fct_claims").fetchdf()  # pandas
pl_df = con.execute("SELECT * FROM fct_claims").pl()    # polars
```

## Further Reading

- [[bigquery-guide]] -- The target cloud warehouse
- [[dataform-guide]] -- Dataform uses SQLX, which is close to what DuckDB SQL can port to
- [[etl-vs-elt]] -- DuckDB enables the "transform after load" pattern locally
