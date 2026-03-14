-- Staging: claims
-- Source: raw_claims (loaded from claims.csv)
-- Clean types, handle nullable close_date, compute report_delay_days.

CREATE OR REPLACE TABLE stg_claims AS
SELECT
    CAST(claim_id AS INTEGER) AS claim_id,
    CAST(policy_id AS INTEGER) AS policy_id,
    TRIM(claim_number) AS claim_number,
    CAST(accident_date AS DATE) AS accident_date,
    CAST(report_date AS DATE) AS report_date,
    -- close_date is NULL for open/reopened claims (read_csv_auto handles this)
    CAST(close_date AS DATE) AS close_date,
    LOWER(TRIM(claim_status)) AS claim_status,
    LOWER(TRIM(cause_of_loss)) AS cause_of_loss,
    CAST(initial_reserve AS DECIMAL(16, 2)) AS initial_reserve,
    CAST(current_reserve AS DECIMAL(16, 2)) AS current_reserve,
    -- Derived fields
    CAST(report_date AS DATE) - CAST(accident_date AS DATE) AS report_delay_days,
    EXTRACT(YEAR FROM CAST(accident_date AS DATE)) AS accident_year,
    EXTRACT(QUARTER FROM CAST(accident_date AS DATE)) AS accident_quarter
FROM raw_claims
WHERE claim_id IS NOT NULL;
