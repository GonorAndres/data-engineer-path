-- Mart: claim payments fact table
-- One row per payment transaction with dimension keys and development context.
-- Grain: one payment event.

CREATE OR REPLACE TABLE fct_claim_payments AS
SELECT
    p.payment_id,
    p.claim_id,
    -- Date keys
    CAST(STRFTIME(p.payment_date, '%Y%m%d') AS INTEGER) AS payment_date_key,
    CAST(STRFTIME(p.accident_date, '%Y%m%d') AS INTEGER) AS accident_date_key,
    -- Dates
    p.payment_date,
    p.accident_date,
    -- Development context (critical for loss triangles)
    p.development_year,
    p.development_months,
    p.accident_year,
    -- Payment metrics
    p.payment_amount,
    p.payment_type,
    p.cumulative_paid,
    p.payment_rank,
    -- Coverage context
    p.coverage_type,
    p.claim_status
FROM int_claim_payments_cumulative p;
