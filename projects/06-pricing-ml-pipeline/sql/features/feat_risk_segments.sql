-- Feature: Risk segmentation
-- Combines policy experience with policyholder demographics for risk grouping.
-- Source: feat_policy_experience JOIN dim_policyholder
-- Uses DuckDB-compatible NTILE for percentile grouping.

CREATE OR REPLACE TABLE feat_risk_segments AS
WITH state_loss_ratios AS (
    -- Compute historical loss ratio by state
    SELECT
        dph.state_code,
        SUM(fpe.total_incurred) AS state_total_incurred,
        SUM(fpe.earned_premium) AS state_earned_premium,
        CASE
            WHEN SUM(fpe.earned_premium) > 0
            THEN SUM(fpe.total_incurred) / SUM(fpe.earned_premium)
            ELSE 0.0
        END AS state_loss_ratio
    FROM feat_policy_experience fpe
    INNER JOIN dim_policyholder dph ON fpe.policyholder_id = dph.policyholder_id
    GROUP BY dph.state_code
),
state_risk AS (
    SELECT
        state_code,
        state_loss_ratio,
        NTILE(3) OVER (ORDER BY state_loss_ratio) AS state_risk_tile
    FROM state_loss_ratios
),
occupation_loss_ratios AS (
    -- Compute historical loss ratio by occupation
    SELECT
        dph.occupation,
        SUM(fpe.total_incurred) AS occ_total_incurred,
        SUM(fpe.earned_premium) AS occ_earned_premium,
        CASE
            WHEN SUM(fpe.earned_premium) > 0
            THEN SUM(fpe.total_incurred) / SUM(fpe.earned_premium)
            ELSE 0.0
        END AS occ_loss_ratio
    FROM feat_policy_experience fpe
    INNER JOIN dim_policyholder dph ON fpe.policyholder_id = dph.policyholder_id
    GROUP BY dph.occupation
),
occupation_risk AS (
    SELECT
        occupation,
        occ_loss_ratio,
        NTILE(3) OVER (ORDER BY occ_loss_ratio) AS occ_risk_tile
    FROM occupation_loss_ratios
)
SELECT
    fpe.policy_id,
    fpe.policyholder_id,
    -- Age band
    CASE
        WHEN dph.current_age BETWEEN 18 AND 25 THEN '18-25'
        WHEN dph.current_age BETWEEN 26 AND 35 THEN '26-35'
        WHEN dph.current_age BETWEEN 36 AND 45 THEN '36-45'
        WHEN dph.current_age BETWEEN 46 AND 55 THEN '46-55'
        WHEN dph.current_age BETWEEN 56 AND 65 THEN '56-65'
        ELSE '65+'
    END AS age_band,
    dph.state_code,
    CASE
        WHEN sr.state_risk_tile = 1 THEN 'low'
        WHEN sr.state_risk_tile = 2 THEN 'medium'
        ELSE 'high'
    END AS state_risk_group,
    dph.occupation,
    CASE
        WHEN orr.occ_risk_tile = 1 THEN 'low'
        WHEN orr.occ_risk_tile = 2 THEN 'medium'
        ELSE 'high'
    END AS occupation_risk_group,
    fpe.coverage_type,
    CASE
        WHEN fpe.deductible <= 5000 THEN 'low'
        WHEN fpe.deductible <= 15000 THEN 'medium'
        ELSE 'high'
    END AS deductible_band,
    CASE
        WHEN NTILE(4) OVER (ORDER BY fpe.annual_premium) = 1 THEN 'Q1'
        WHEN NTILE(4) OVER (ORDER BY fpe.annual_premium) = 2 THEN 'Q2'
        WHEN NTILE(4) OVER (ORDER BY fpe.annual_premium) = 3 THEN 'Q3'
        ELSE 'Q4'
    END AS premium_band,
    dph.gender
FROM feat_policy_experience fpe
INNER JOIN dim_policyholder dph ON fpe.policyholder_id = dph.policyholder_id
LEFT JOIN state_risk sr ON dph.state_code = sr.state_code
LEFT JOIN occupation_risk orr ON dph.occupation = orr.occupation;
