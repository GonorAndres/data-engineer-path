-- Feature: Policy-level experience features
-- One row per policy_id with exposure, claims, and severity metrics.
-- Source: dim_policy LEFT JOIN int_policy_exposure LEFT JOIN fct_claims (aggregated)

CREATE OR REPLACE TABLE feat_policy_experience AS
WITH policy_claims AS (
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        SUM(total_paid) AS total_paid,
        SUM(incurred_amount) AS total_incurred,
        AVG(CASE WHEN total_paid > 0 THEN total_paid ELSE NULL END) AS avg_severity,
        MAX(CASE WHEN total_paid > 0 THEN total_paid ELSE NULL END) AS max_severity,
        MAX(accident_date) AS last_claim_date
    FROM fct_claims
    GROUP BY policy_id
),
policy_exposure AS (
    SELECT
        policy_id,
        SUM(exposure_years) AS exposure_years,
        SUM(earned_premium) AS earned_premium
    FROM int_policy_exposure
    GROUP BY policy_id
)
SELECT
    dp.policy_id,
    dp.policyholder_id,
    dp.coverage_type,
    dp.annual_premium,
    dp.deductible,
    dp.coverage_limit,
    dp.status AS policy_status,
    COALESCE(pe.exposure_years, 0.0) AS exposure_years,
    COALESCE(pe.earned_premium, 0.0) AS earned_premium,
    COALESCE(pc.claim_count, 0) AS claim_count,
    CASE
        WHEN COALESCE(pe.exposure_years, 0.0) > 0
        THEN CAST(COALESCE(pc.claim_count, 0) AS DOUBLE) / pe.exposure_years
        ELSE 0.0
    END AS claim_frequency,
    COALESCE(pc.total_paid, 0.0) AS total_paid,
    COALESCE(pc.total_incurred, 0.0) AS total_incurred,
    COALESCE(pc.avg_severity, 0.0) AS avg_severity,
    COALESCE(pc.max_severity, 0.0) AS max_severity,
    CASE
        WHEN COALESCE(pc.max_severity, 0.0) > 200000 THEN TRUE
        ELSE FALSE
    END AS has_large_loss,
    CASE
        WHEN pc.last_claim_date IS NOT NULL
        THEN DATE '2025-12-31' - pc.last_claim_date
        ELSE NULL
    END AS days_since_last_claim
FROM dim_policy dp
LEFT JOIN policy_exposure pe ON dp.policy_id = pe.policy_id
LEFT JOIN policy_claims pc ON dp.policy_id = pc.policy_id;
