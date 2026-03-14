-- Intermediate: policy exposure calculations
-- Computes earned exposure for frequency analysis.
-- Clips exposure to observation window [2020-01-01, 2025-12-31].

CREATE OR REPLACE TABLE int_policy_exposure AS
WITH exposure_calc AS (
    SELECT
        policy_id,
        policyholder_id,
        policy_number,
        coverage_type,
        effective_date,
        expiration_date,
        annual_premium,
        status,
        -- Clip policy period to observation window
        GREATEST(effective_date, DATE '2020-01-01') AS exposure_start,
        LEAST(expiration_date, DATE '2025-12-31') AS exposure_end
    FROM stg_policies
    WHERE status != 'cancelled'
)
SELECT
    *,
    -- Earned exposure in days
    CASE
        WHEN exposure_end > exposure_start
        THEN exposure_end - exposure_start
        ELSE 0
    END AS exposure_days,
    -- Earned exposure in years (for frequency = claims / exposure_years)
    CASE
        WHEN exposure_end > exposure_start
        THEN CAST(exposure_end - exposure_start AS DOUBLE) / 365.25
        ELSE 0.0
    END AS exposure_years,
    -- Earned premium (pro-rata)
    CASE
        WHEN exposure_end > exposure_start
        THEN ROUND(
            annual_premium
            * CAST(exposure_end - exposure_start AS DOUBLE)
            / CAST(expiration_date - effective_date AS DOUBLE),
            2
        )
        ELSE 0.0
    END AS earned_premium,
    -- Exposure year for aggregation
    EXTRACT(YEAR FROM GREATEST(effective_date, DATE '2020-01-01')) AS exposure_year
FROM exposure_calc
WHERE exposure_end > exposure_start;
