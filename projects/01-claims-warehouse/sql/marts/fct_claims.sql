-- Mart: claims fact table
-- One row per claim with all metrics and dimension keys.
-- Grain: one claim event.

CREATE OR REPLACE TABLE fct_claims AS
WITH claim_payments_summary AS (
    SELECT
        claim_id,
        SUM(CASE WHEN payment_type = 'indemnity' THEN payment_amount ELSE 0 END) AS total_indemnity,
        SUM(CASE WHEN payment_type = 'expense' THEN payment_amount ELSE 0 END) AS total_expense,
        SUM(CASE WHEN payment_type = 'recovery' THEN payment_amount ELSE 0 END) AS total_recovery,
        SUM(payment_amount) AS total_paid,
        COUNT(*) AS payment_count,
        MIN(payment_date) AS first_payment_date,
        MAX(payment_date) AS last_payment_date
    FROM stg_claim_payments
    GROUP BY claim_id
)
SELECT
    c.claim_id,
    c.policy_id,
    c.policyholder_id,
    c.claim_number,
    -- Date keys for joining to dim_date
    CAST(STRFTIME(c.accident_date, '%Y%m%d') AS INTEGER) AS accident_date_key,
    CAST(STRFTIME(c.report_date, '%Y%m%d') AS INTEGER) AS report_date_key,
    -- Dates
    c.accident_date,
    c.report_date,
    c.close_date,
    -- Attributes
    c.claim_status,
    c.cause_of_loss,
    c.coverage_type,
    c.coverage_category,
    c.accident_year,
    c.accident_quarter,
    c.report_delay_days,
    c.development_months_at_valuation,
    c.is_large_loss,
    -- Policy context
    c.annual_premium,
    c.deductible,
    c.coverage_limit,
    -- Reserve metrics
    c.initial_reserve,
    c.current_reserve,
    -- Payment metrics
    COALESCE(ps.total_indemnity, 0) AS total_indemnity,
    COALESCE(ps.total_expense, 0) AS total_expense,
    COALESCE(ps.total_recovery, 0) AS total_recovery,
    COALESCE(ps.total_paid, 0) AS total_paid,
    COALESCE(ps.payment_count, 0) AS payment_count,
    ps.first_payment_date,
    ps.last_payment_date,
    -- Incurred = paid + outstanding reserve
    COALESCE(ps.total_paid, 0) + c.current_reserve AS incurred_amount,
    -- Net paid = total paid + recoveries (recoveries are negative)
    COALESCE(ps.total_paid, 0) AS net_paid
FROM int_claims_enriched c
LEFT JOIN claim_payments_summary ps ON c.claim_id = ps.claim_id;
