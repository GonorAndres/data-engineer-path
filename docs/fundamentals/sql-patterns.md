---
tags: [fundamentals, sql, patterns]
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# SQL Patterns for Data Engineering

SQL is the lingua franca of data engineering. You'll write it in dbt models, BigQuery, Dataform, ad-hoc analysis, and data quality checks. This doc covers patterns beyond basic SELECT that show up constantly in DE work.

## CTEs: The Foundation of Readable SQL

Common Table Expressions (CTEs) replace subqueries and make SQL readable like a pipeline.

```sql
-- BAD: Nested subqueries
SELECT * FROM (
  SELECT * FROM (
    SELECT * FROM claims WHERE status = 'open'
  ) WHERE amount > 1000
) WHERE region = 'WEST';

-- GOOD: CTEs read top-to-bottom
WITH open_claims AS (
  SELECT * FROM claims WHERE status = 'open'
),
large_claims AS (
  SELECT * FROM open_claims WHERE amount > 1000
)
SELECT * FROM large_claims WHERE region = 'WEST';
```

**Rule:** If you're nesting more than one subquery, use CTEs.

## Window Functions

The most powerful SQL feature for analytics. They compute values across a set of rows related to the current row, without collapsing rows like GROUP BY.

### Pattern: Running Totals

```sql
-- Cumulative paid amount per claim over development periods
SELECT
  claim_id,
  development_month,
  paid_amount,
  SUM(paid_amount) OVER (
    PARTITION BY claim_id
    ORDER BY development_month
  ) AS cumulative_paid
FROM claim_payments;
```

### Pattern: Rank / Row Number

```sql
-- Latest record per policy (deduplication)
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY policy_id
      ORDER BY updated_at DESC
    ) AS rn
  FROM policy_snapshots
)
SELECT * FROM ranked WHERE rn = 1;
```

### Pattern: LAG / LEAD (Previous/Next Row)

```sql
-- Month-over-month change in incurred losses
SELECT
  accident_month,
  incurred_loss,
  LAG(incurred_loss) OVER (ORDER BY accident_month) AS prev_month_loss,
  incurred_loss - LAG(incurred_loss) OVER (ORDER BY accident_month) AS mom_change
FROM monthly_losses;
```

### Pattern: Moving Averages

```sql
-- 3-month rolling average of claim frequency
SELECT
  month,
  claim_count,
  AVG(claim_count) OVER (
    ORDER BY month
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS rolling_3m_avg
FROM monthly_claim_counts;
```

## QUALIFY (BigQuery)

BigQuery-specific clause that filters window function results without a subquery. Extremely useful.

```sql
-- Latest record per policy, no CTE needed
SELECT *
FROM policy_snapshots
QUALIFY ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY updated_at DESC) = 1;
```

**Note:** This is BigQuery/Snowflake syntax. Not available in PostgreSQL, DuckDB (v0.9+), or Redshift.

## Pivoting and Unpivoting

### Pivot: Rows to Columns (Loss Triangles)

```sql
-- Build a development triangle
SELECT
  accident_year,
  SUM(IF(development_year = 1, paid_loss, 0)) AS dev_1,
  SUM(IF(development_year = 2, paid_loss, 0)) AS dev_2,
  SUM(IF(development_year = 3, paid_loss, 0)) AS dev_3
FROM claim_triangles
GROUP BY accident_year
ORDER BY accident_year;
```

### Unpivot: Columns to Rows

```sql
-- Normalize a wide table
SELECT claim_id, metric_name, metric_value
FROM claims
UNPIVOT (metric_value FOR metric_name IN (paid_amount, reserved_amount, incurred_amount));
```

## MERGE (Upsert)

Critical for incremental loads -- update existing rows, insert new ones.

```sql
MERGE INTO dim_policyholder AS target
USING stg_policyholder_updates AS source
ON target.policyholder_id = source.policyholder_id
WHEN MATCHED THEN
  UPDATE SET
    target.name = source.name,
    target.address = source.address,
    target.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (policyholder_id, name, address, created_at, updated_at)
  VALUES (source.policyholder_id, source.name, source.address, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
```

## Anti-Join Pattern

Find rows in one table that DON'T exist in another.

```sql
-- Policies with no claims
SELECT p.*
FROM policies p
LEFT JOIN claims c ON p.policy_id = c.policy_id
WHERE c.policy_id IS NULL;

-- Alternative using NOT EXISTS (often faster in BigQuery)
SELECT p.*
FROM policies p
WHERE NOT EXISTS (
  SELECT 1 FROM claims c WHERE c.policy_id = p.policy_id
);
```

## Date Spine

Generate a complete date range to avoid gaps in time-series data.

```sql
-- BigQuery date spine
WITH date_spine AS (
  SELECT date
  FROM UNNEST(
    GENERATE_DATE_ARRAY('2020-01-01', CURRENT_DATE(), INTERVAL 1 DAY)
  ) AS date
)
SELECT
  ds.date,
  COALESCE(c.claim_count, 0) AS claim_count
FROM date_spine ds
LEFT JOIN daily_claim_counts c ON ds.date = c.claim_date;
```

## Related
- [[data-modeling-overview]] -- How these patterns fit into dimensional models
- [[bigquery-guide]] -- BigQuery-specific SQL optimizations
- [[etl-vs-elt]] -- SQL patterns are the core of ELT transforms
