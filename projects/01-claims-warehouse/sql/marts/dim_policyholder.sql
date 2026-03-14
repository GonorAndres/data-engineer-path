-- Mart: policyholder dimension
-- One row per policyholder with derived attributes.

CREATE OR REPLACE TABLE dim_policyholder AS
SELECT
    policyholder_id,
    first_name,
    last_name,
    first_name || ' ' || last_name AS full_name,
    date_of_birth,
    -- Age at registration
    EXTRACT(YEAR FROM AGE(registration_date, date_of_birth)) AS age_at_registration,
    -- Current age (relative to valuation date)
    EXTRACT(YEAR FROM AGE(DATE '2025-12-31', date_of_birth)) AS current_age,
    gender,
    state_code,
    city,
    occupation,
    registration_date
FROM stg_policyholders;
