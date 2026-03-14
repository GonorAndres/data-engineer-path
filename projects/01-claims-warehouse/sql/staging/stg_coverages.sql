-- Staging: coverages
-- Source: raw_coverages (loaded from coverages.csv)
-- Clean types for coverage reference data.

CREATE OR REPLACE TABLE stg_coverages AS
SELECT
    LOWER(TRIM(coverage_type)) AS coverage_type,
    LOWER(TRIM(coverage_category)) AS coverage_category,
    TRIM(description) AS description
FROM raw_coverages;
