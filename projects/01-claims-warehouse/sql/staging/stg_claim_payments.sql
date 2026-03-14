-- Staging: claim_payments
-- Source: raw_claim_payments (loaded from claim_payments.csv)
-- Clean types.

CREATE OR REPLACE TABLE stg_claim_payments AS
SELECT
    CAST(payment_id AS INTEGER) AS payment_id,
    CAST(claim_id AS INTEGER) AS claim_id,
    CAST(payment_date AS DATE) AS payment_date,
    CAST(payment_amount AS DECIMAL(16, 2)) AS payment_amount,
    LOWER(TRIM(payment_type)) AS payment_type,
    CAST(cumulative_paid AS DECIMAL(16, 2)) AS cumulative_paid
FROM raw_claim_payments
WHERE payment_id IS NOT NULL;
