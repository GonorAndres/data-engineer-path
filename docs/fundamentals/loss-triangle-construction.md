---
tags: [fundamentals, actuarial, loss-triangles, reserving]
status: draft
created: 2026-03-14
updated: 2026-03-14
---

# Loss Triangle Construction

Loss triangles (development triangles) are the foundational tool for actuarial reserving -- estimating how much money an insurance company needs to set aside for claims that are reported but not fully paid, or not yet reported at all (IBNR).

This is the **differentiator** in a DE portfolio. No other data engineering candidate builds loss triangles.

## Why Loss Triangles Matter

Insurance claims take time to develop:
- An auto collision is reported in a week, paid in months
- A liability claim may take years to settle
- A medical claim might reopen with complications

At any point in time, the insurer needs to know: **how much will we ultimately pay for all claims from each accident period?** The difference between what's been paid and the estimated ultimate is the **reserve**.

## Anatomy of a Loss Triangle

```
                     Development Year
Accident  ┌──────┬──────┬──────┬──────┬──────┐
  Year    │ Dev 0│ Dev 1│ Dev 2│ Dev 3│ Dev 4│
├─────────┼──────┼──────┼──────┼──────┼──────┤
│  2021   │  500 │ 1200 │ 1600 │ 1800 │ 1850 │  <- fully developed
│  2022   │  600 │ 1400 │ 1750 │ 1900 │      │  <- almost done
│  2023   │  550 │ 1300 │ 1650 │      │      │  <- developing
│  2024   │  580 │ 1350 │      │      │      │  <- early stage
│  2025   │  620 │      │      │      │      │  <- just started
└─────────┴──────┴──────┴──────┴──────┴──────┘
         The diagonal ↗ is the latest valuation
         Empty cells = future development (IBNR)
```

**Key concepts:**
- **Rows** = accident periods (when losses occurred)
- **Columns** = development periods (time since accident)
- **Values** = cumulative paid (or incurred) amounts
- **Diagonal** = the most recent data point for each accident year
- **Upper-left triangle** = observed data
- **Lower-right** = future development we need to estimate

## Building a Loss Triangle in SQL

### Step 1: Compute Development Year

```sql
-- Development year = calendar year of payment - accident year
SELECT
    EXTRACT(YEAR FROM c.accident_date) AS accident_year,
    EXTRACT(YEAR FROM p.payment_date) - EXTRACT(YEAR FROM c.accident_date)
        AS development_year,
    p.payment_amount
FROM claims c
JOIN claim_payments p ON c.claim_id = p.claim_id;
```

### Step 2: Aggregate and Cumulate

```sql
WITH incremental AS (
    SELECT
        accident_year,
        development_year,
        SUM(payment_amount) AS incremental_paid
    FROM claim_development
    GROUP BY accident_year, development_year
),
cumulative AS (
    SELECT
        accident_year,
        development_year,
        SUM(incremental_paid) OVER (
            PARTITION BY accident_year
            ORDER BY development_year
        ) AS cumulative_paid
    FROM incremental
)
```

### Step 3: Pivot to Triangle Format

```sql
SELECT
    accident_year,
    MAX(CASE WHEN development_year = 0 THEN cumulative_paid END) AS dev_0,
    MAX(CASE WHEN development_year = 1 THEN cumulative_paid END) AS dev_1,
    MAX(CASE WHEN development_year = 2 THEN cumulative_paid END) AS dev_2,
    -- ...
FROM cumulative
GROUP BY accident_year
ORDER BY accident_year;
```

This is exactly what `rpt_loss_triangle.sql` in [[projects/01-claims-warehouse]] does.

## Development Factors

From the triangle, actuaries compute **link ratios** (development factors):

```
Factor(0->1) = Cumulative at Dev 1 / Cumulative at Dev 0
```

Using the example above:
- 2021: 1200/500 = 2.40
- 2022: 1400/600 = 2.33
- 2023: 1300/550 = 2.36
- 2024: 1350/580 = 2.33
- Weighted average: ~2.35

These factors project incomplete years to their ultimate value:
- 2025 ultimate estimate: 620 * 2.35 * (next factors) = projected ultimate

## IBNR (Incurred But Not Reported)

IBNR is the difference between estimated ultimate losses and current incurred losses:

```
IBNR = Estimated Ultimate - (Paid + Case Reserves)
```

This is why recent accident years in the triangle have less data -- not all claims have been reported or paid yet.

In the data generator for [[projects/01-claims-warehouse]], the IBNR effect is modeled explicitly: claims whose report date would fall after the valuation date are not generated.

## For Data Engineers

As a DE building insurance data pipelines, you need to:

1. **Structure the data** so actuaries can compute triangles (accident date + payment date + amounts)
2. **Partition/cluster** BigQuery tables by accident year for performance
3. **Handle late-arriving data** -- claims can reopen, payments arrive out of order
4. **Maintain history** -- every valuation is a snapshot; triangles change over time
5. **Automate refresh** -- triangles should update when new payment data arrives

The star schema in [[data-modeling-overview]] is designed around this: `fct_claims` has the accident date, `fct_claim_payments` has payment dates and development periods, and `dim_date` provides the calendar.

## Further Reading

- [[sql-patterns]] -- Window functions and pivoting used in triangle queries
- [[data-quality]] -- Triangle quality depends on clean accident/payment dates
- [[batch-vs-stream]] -- Triangles are a batch analytics pattern
