-- Mart: coverage type dimension
-- Static reference table for coverage types.

CREATE OR REPLACE TABLE dim_coverage AS
SELECT
    -- Surrogate key
    ROW_NUMBER() OVER (ORDER BY coverage_type) AS coverage_key,
    coverage_type,
    coverage_category,
    description
FROM stg_coverages;
