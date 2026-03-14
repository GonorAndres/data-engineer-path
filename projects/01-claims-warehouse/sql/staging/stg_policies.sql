-- Staging: policies
-- Source: raw_policies (loaded from policies.csv)
-- Clean types, compute policy_term_days.

CREATE OR REPLACE TABLE stg_policies AS
SELECT
    CAST(policy_id AS INTEGER) AS policy_id,
    CAST(policyholder_id AS INTEGER) AS policyholder_id,
    TRIM(policy_number) AS policy_number,
    LOWER(TRIM(coverage_type)) AS coverage_type,
    CAST(effective_date AS DATE) AS effective_date,
    CAST(expiration_date AS DATE) AS expiration_date,
    CAST(annual_premium AS DECIMAL(14, 2)) AS annual_premium,
    CAST(deductible AS DECIMAL(14, 2)) AS deductible,
    CAST(coverage_limit AS DECIMAL(16, 2)) AS coverage_limit,
    LOWER(TRIM(status)) AS status,
    -- Derived: policy term in days
    CAST(expiration_date AS DATE) - CAST(effective_date AS DATE) AS policy_term_days
FROM raw_policies
WHERE policy_id IS NOT NULL
  AND CAST(effective_date AS DATE) < CAST(expiration_date AS DATE);
