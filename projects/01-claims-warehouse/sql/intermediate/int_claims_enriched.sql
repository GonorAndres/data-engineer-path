-- Intermediate: claims enriched with policy and coverage context
-- Joins claims with their policy and coverage details.
-- Computes development months relative to accident date.

CREATE OR REPLACE TABLE int_claims_enriched AS
WITH claim_policy AS (
    SELECT
        c.claim_id,
        c.policy_id,
        c.claim_number,
        c.accident_date,
        c.report_date,
        c.close_date,
        c.claim_status,
        c.cause_of_loss,
        c.initial_reserve,
        c.current_reserve,
        c.report_delay_days,
        c.accident_year,
        c.accident_quarter,
        p.policyholder_id,
        p.policy_number,
        p.coverage_type,
        p.effective_date AS policy_effective_date,
        p.expiration_date AS policy_expiration_date,
        p.annual_premium,
        p.deductible,
        p.coverage_limit,
        p.status AS policy_status,
        cov.coverage_category,
        cov.description AS coverage_description
    FROM stg_claims c
    INNER JOIN stg_policies p ON c.policy_id = p.policy_id
    LEFT JOIN stg_coverages cov ON p.coverage_type = cov.coverage_type
)
SELECT
    *,
    -- Months between accident and valuation date (2025-12-31)
    (EXTRACT(YEAR FROM DATE '2025-12-31') - EXTRACT(YEAR FROM accident_date)) * 12
        + (EXTRACT(MONTH FROM DATE '2025-12-31') - EXTRACT(MONTH FROM accident_date))
        AS development_months_at_valuation,
    -- Is this a large loss? (above 90th percentile reserve)
    CASE
        WHEN initial_reserve > 200000 THEN TRUE
        ELSE FALSE
    END AS is_large_loss
FROM claim_policy;
