-- Model: Final training dataset
-- Joins feature tables into one modeling-ready dataset.
-- Source: feat_policy_experience JOIN feat_risk_segments JOIN dim_policy LEFT JOIN latest benchmarks
-- Filters: exposure > 0.25 years, non-cancelled policies
-- Split: train if policy_year <= 2023, else test

CREATE OR REPLACE TABLE model_training_set AS
WITH latest_benchmarks AS (
    -- Get the most recent benchmark year per coverage type
    SELECT
        coverage_type,
        benchmark_frequency,
        benchmark_severity,
        frequency_trend,
        severity_trend,
        ROW_NUMBER() OVER (PARTITION BY coverage_type ORDER BY year DESC) AS rn
    FROM feat_historical_benchmarks
    WHERE benchmark_frequency IS NOT NULL
)
SELECT
    fpe.policy_id,
    -- Targets
    fpe.claim_frequency AS target_claim_frequency,
    CASE
        WHEN fpe.exposure_years > 0
        THEN fpe.total_incurred / fpe.exposure_years
        ELSE 0.0
    END AS target_pure_premium,
    CASE WHEN fpe.claim_count > 0 THEN 1 ELSE 0 END AS target_has_claim,
    -- Exposure
    fpe.exposure_years,
    -- Log-transformed continuous features (add small constant to avoid log(0))
    LN(GREATEST(fpe.annual_premium, 1.0)) AS log_premium,
    LN(GREATEST(fpe.deductible, 1.0)) AS log_deductible,
    LN(GREATEST(fpe.coverage_limit, 1.0)) AS log_coverage_limit,
    -- Categorical features from risk segments
    frs.age_band,
    frs.state_risk_group,
    frs.occupation_risk_group,
    fpe.coverage_type,
    frs.deductible_band,
    frs.premium_band,
    frs.gender,
    -- Benchmark features
    COALESCE(lb.benchmark_frequency, 0.0) AS benchmark_frequency,
    COALESCE(lb.benchmark_severity, 0.0) AS benchmark_severity,
    COALESCE(lb.frequency_trend, 0.0) AS frequency_trend,
    COALESCE(lb.severity_trend, 0.0) AS severity_trend,
    -- Experience features
    fpe.claim_count,
    fpe.avg_severity,
    fpe.has_large_loss,
    -- Train/test split based on policy year
    CASE
        WHEN dp.policy_year <= 2023 THEN 'train'
        ELSE 'test'
    END AS split
FROM feat_policy_experience fpe
INNER JOIN feat_risk_segments frs ON fpe.policy_id = frs.policy_id
INNER JOIN dim_policy dp ON fpe.policy_id = dp.policy_id
LEFT JOIN latest_benchmarks lb
    ON fpe.coverage_type = lb.coverage_type
    AND lb.rn = 1
WHERE fpe.exposure_years > 0.25
  AND fpe.policy_status != 'cancelled';
