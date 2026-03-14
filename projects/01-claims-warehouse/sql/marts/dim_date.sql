-- Mart: date dimension
-- Standard date spine from 2019-01-01 to 2026-12-31.
-- Covers all possible dates in the claims data plus buffer.

CREATE OR REPLACE TABLE dim_date AS
WITH date_spine AS (
    SELECT
        UNNEST(generate_series(DATE '2019-01-01', DATE '2026-12-31', INTERVAL 1 DAY))::DATE AS full_date
)
SELECT
    -- Surrogate key: YYYYMMDD integer
    CAST(STRFTIME(full_date, '%Y%m%d') AS INTEGER) AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date) AS year,
    EXTRACT(QUARTER FROM full_date) AS quarter,
    EXTRACT(MONTH FROM full_date) AS month,
    EXTRACT(DAY FROM full_date) AS day,
    EXTRACT(DOW FROM full_date) AS day_of_week,
    STRFTIME(full_date, '%A') AS day_name,
    STRFTIME(full_date, '%B') AS month_name,
    -- Week number (ISO)
    EXTRACT(WEEK FROM full_date) AS iso_week,
    -- Flags
    CASE WHEN EXTRACT(DOW FROM full_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN full_date = LAST_DAY(full_date) THEN TRUE ELSE FALSE END AS is_month_end,
    CASE
        WHEN EXTRACT(MONTH FROM full_date) IN (3, 6, 9, 12)
         AND full_date = LAST_DAY(full_date)
        THEN TRUE ELSE FALSE
    END AS is_quarter_end,
    CASE
        WHEN EXTRACT(MONTH FROM full_date) = 12
         AND EXTRACT(DAY FROM full_date) = 31
        THEN TRUE ELSE FALSE
    END AS is_year_end,
    -- Mexican fiscal year = calendar year
    EXTRACT(YEAR FROM full_date) AS fiscal_year,
    EXTRACT(QUARTER FROM full_date) AS fiscal_quarter,
    -- Year-quarter label for reporting
    EXTRACT(YEAR FROM full_date) || '-Q' || EXTRACT(QUARTER FROM full_date) AS year_quarter_label
FROM date_spine
ORDER BY full_date;
