-- Staging: policyholders
-- Source: raw_policyholders (loaded from policyholders.csv)
-- Clean types, trim strings, validate required fields.

CREATE OR REPLACE TABLE stg_policyholders AS
SELECT
    CAST(policyholder_id AS INTEGER) AS policyholder_id,
    TRIM(first_name) AS first_name,
    TRIM(last_name) AS last_name,
    CAST(date_of_birth AS DATE) AS date_of_birth,
    UPPER(TRIM(gender)) AS gender,
    UPPER(TRIM(state_code)) AS state_code,
    COALESCE(TRIM(city), 'unknown') AS city,
    COALESCE(LOWER(TRIM(occupation)), 'unknown') AS occupation,
    CAST(registration_date AS DATE) AS registration_date
FROM raw_policyholders
WHERE policyholder_id IS NOT NULL;
