-- Mart: policy dimension
-- One row per policy with coverage details.

CREATE OR REPLACE TABLE dim_policy AS
SELECT
    p.policy_id,
    p.policyholder_id,
    p.policy_number,
    p.coverage_type,
    c.coverage_category,
    c.description AS coverage_description,
    p.effective_date,
    p.expiration_date,
    p.policy_term_days,
    p.annual_premium,
    p.deductible,
    p.coverage_limit,
    p.status,
    EXTRACT(YEAR FROM p.effective_date) AS policy_year,
    -- Is the policy currently active?
    CASE
        WHEN p.status = 'active'
         AND p.effective_date <= DATE '2025-12-31'
         AND p.expiration_date > DATE '2025-12-31'
        THEN TRUE ELSE FALSE
    END AS is_currently_active
FROM stg_policies p
LEFT JOIN stg_coverages c ON p.coverage_type = c.coverage_type;
