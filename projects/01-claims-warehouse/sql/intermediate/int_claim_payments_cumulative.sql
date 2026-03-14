-- Intermediate: claim payments with cumulative totals and development context
-- Recomputes cumulative paid using window functions for consistency.
-- Adds development period relative to accident date.

CREATE OR REPLACE TABLE int_claim_payments_cumulative AS
WITH payments_with_claim AS (
    SELECT
        p.payment_id,
        p.claim_id,
        p.payment_date,
        p.payment_amount,
        p.payment_type,
        c.accident_date,
        c.accident_year,
        c.coverage_type,
        c.claim_status
    FROM stg_claim_payments p
    INNER JOIN (
        SELECT
            sc.claim_id,
            sc.accident_date,
            sc.accident_year,
            sp.coverage_type,
            sc.claim_status
        FROM stg_claims sc
        INNER JOIN stg_policies sp ON sc.policy_id = sp.policy_id
    ) c ON p.claim_id = c.claim_id
)
SELECT
    payment_id,
    claim_id,
    payment_date,
    payment_amount,
    payment_type,
    accident_date,
    accident_year,
    coverage_type,
    claim_status,
    -- Development year: calendar year of payment minus accident year
    EXTRACT(YEAR FROM payment_date) - accident_year AS development_year,
    -- Development months from accident
    (EXTRACT(YEAR FROM payment_date) - EXTRACT(YEAR FROM accident_date)) * 12
        + (EXTRACT(MONTH FROM payment_date) - EXTRACT(MONTH FROM accident_date))
        AS development_months,
    -- Recompute cumulative paid per claim using window function
    SUM(payment_amount) OVER (
        PARTITION BY claim_id
        ORDER BY payment_date, payment_id
    ) AS cumulative_paid,
    -- Payment rank within claim
    ROW_NUMBER() OVER (
        PARTITION BY claim_id
        ORDER BY payment_date, payment_id
    ) AS payment_rank
FROM payments_with_claim;
