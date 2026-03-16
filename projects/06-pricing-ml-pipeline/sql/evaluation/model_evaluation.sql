-- Evaluation: Segment-level model performance metrics
-- Compares predicted vs actual pure premium by split and coverage type.
-- Source: model_training_set JOIN model_predictions

CREATE OR REPLACE TABLE model_evaluation AS
SELECT
    mts.split,
    mts.coverage_type,
    COUNT(*) AS policy_count,
    ROUND(AVG(mts.target_pure_premium), 2) AS avg_actual_pp,
    ROUND(AVG(mp.predicted_pure_premium), 2) AS avg_predicted_pp,
    ROUND(AVG(mp.predicted_pure_premium - mts.target_pure_premium), 2) AS avg_residual,
    ROUND(AVG(ABS(mp.predicted_pure_premium - mts.target_pure_premium)), 2) AS mae,
    ROUND(SQRT(AVG(POWER(mp.predicted_pure_premium - mts.target_pure_premium, 2))), 2) AS rmse,
    -- Actual-to-Expected ratio (A/E): sum(predicted * exposure) / sum(actual * exposure)
    CASE
        WHEN SUM(mts.target_pure_premium * mts.exposure_years) > 0
        THEN ROUND(
            SUM(mp.predicted_pure_premium * mts.exposure_years)
            / SUM(mts.target_pure_premium * mts.exposure_years),
            4
        )
        ELSE NULL
    END AS ae_ratio
FROM model_training_set mts
INNER JOIN model_predictions mp ON mts.policy_id = mp.policy_id
GROUP BY mts.split, mts.coverage_type
ORDER BY mts.split, mts.coverage_type;
