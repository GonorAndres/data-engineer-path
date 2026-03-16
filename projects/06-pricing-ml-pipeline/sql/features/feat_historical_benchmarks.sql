-- Feature: Coverage-level historical benchmarks
-- Year-over-year frequency and severity benchmarks from rpt_claim_frequency.
-- Source: rpt_claim_frequency

CREATE OR REPLACE TABLE feat_historical_benchmarks AS
WITH benchmarks AS (
    SELECT
        year,
        coverage_type,
        claim_frequency AS benchmark_frequency,
        avg_severity AS benchmark_severity,
        pure_premium AS benchmark_pure_premium,
        loss_ratio AS benchmark_loss_ratio
    FROM rpt_claim_frequency
)
SELECT
    b.year,
    b.coverage_type,
    b.benchmark_frequency,
    b.benchmark_severity,
    b.benchmark_pure_premium,
    b.benchmark_loss_ratio,
    -- Frequency trend: YoY change
    CASE
        WHEN LAG(b.benchmark_frequency) OVER (
            PARTITION BY b.coverage_type ORDER BY b.year
        ) IS NOT NULL AND LAG(b.benchmark_frequency) OVER (
            PARTITION BY b.coverage_type ORDER BY b.year
        ) > 0
        THEN (b.benchmark_frequency - LAG(b.benchmark_frequency) OVER (
            PARTITION BY b.coverage_type ORDER BY b.year
        )) / LAG(b.benchmark_frequency) OVER (
            PARTITION BY b.coverage_type ORDER BY b.year
        )
        ELSE NULL
    END AS frequency_trend,
    -- Severity trend: YoY change
    CASE
        WHEN LAG(b.benchmark_severity) OVER (
            PARTITION BY b.coverage_type ORDER BY b.year
        ) IS NOT NULL AND LAG(b.benchmark_severity) OVER (
            PARTITION BY b.coverage_type ORDER BY b.year
        ) > 0
        THEN (b.benchmark_severity - LAG(b.benchmark_severity) OVER (
            PARTITION BY b.coverage_type ORDER BY b.year
        )) / LAG(b.benchmark_severity) OVER (
            PARTITION BY b.coverage_type ORDER BY b.year
        )
        ELSE NULL
    END AS severity_trend
FROM benchmarks b
ORDER BY b.coverage_type, b.year;
