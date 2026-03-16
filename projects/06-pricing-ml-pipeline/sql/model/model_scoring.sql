-- Model: Pricing adequacy assessment
-- Compares predicted pure premium to actual premium for pricing decisions.
-- Source: model_training_set JOIN feat_policy_experience JOIN model_predictions

CREATE OR REPLACE TABLE model_scoring AS
SELECT
    mts.policy_id,
    mts.coverage_type,
    mts.age_band,
    mts.state_risk_group,
    mp.predicted_pure_premium,
    fpe.annual_premium AS actual_premium,
    mts.exposure_years,
    -- Price adequacy ratio: predicted loss cost / actual premium charged
    CASE
        WHEN fpe.annual_premium > 0
        THEN mp.predicted_pure_premium / fpe.annual_premium
        ELSE NULL
    END AS price_adequacy_ratio,
    -- Pricing assessment
    CASE
        WHEN fpe.annual_premium > 0 AND mp.predicted_pure_premium / fpe.annual_premium > 1.1
        THEN 'underpriced'
        WHEN fpe.annual_premium > 0 AND mp.predicted_pure_premium / fpe.annual_premium < 0.9
        THEN 'overpriced'
        WHEN fpe.annual_premium > 0
        THEN 'adequate'
        ELSE NULL
    END AS pricing_assessment
FROM model_training_set mts
INNER JOIN feat_policy_experience fpe ON mts.policy_id = fpe.policy_id
INNER JOIN model_predictions mp ON mts.policy_id = mp.policy_id;
