with claims_by_year_coverage as (
    select
        accident_year,
        coverage_type,
        count(*) as claim_count,
        sum(total_paid) as total_paid,
        sum(incurred_amount) as total_incurred
    from {{ ref('fct_claims') }}
    group by accident_year, coverage_type
),

exposure_by_year_coverage as (
    select
        exposure_year,
        coverage_type,
        sum(exposure_years) as total_exposure_years,
        sum(earned_premium) as total_earned_premium,
        count(*) as policy_count
    from {{ ref('int_policy_exposure') }}
    group by exposure_year, coverage_type
),

combined as (
    select
        coalesce(c.accident_year, e.exposure_year) as year,
        coalesce(c.coverage_type, e.coverage_type) as coverage_type,
        coalesce(e.policy_count, 0) as policy_count,
        coalesce(e.total_exposure_years, 0) as exposure_years,
        coalesce(e.total_earned_premium, 0) as earned_premium,
        coalesce(c.claim_count, 0) as claim_count,
        coalesce(c.total_paid, 0) as total_paid,
        coalesce(c.total_incurred, 0) as total_incurred
    from claims_by_year_coverage as c
    full outer join exposure_by_year_coverage as e
        on
            c.accident_year = e.exposure_year
            and c.coverage_type = e.coverage_type
)

-- Formulas come from macros/kpis.sql so they are defined once.
select
    year,
    coverage_type,
    policy_count,
    exposure_years,
    earned_premium,
    claim_count,
    total_paid,
    coalesce({{ claims.kpi_severity('total_paid', 'claim_count') }}, 0) as avg_severity,
    total_incurred,
    round({{ claims.kpi_frequency('claim_count', 'exposure_years') }}, 4) as claim_frequency,
    round({{ claims.kpi_pure_premium('total_incurred', 'exposure_years') }}, 2) as pure_premium,
    round({{ claims.kpi_loss_ratio('total_incurred', 'earned_premium') }}, 4) as loss_ratio
from combined
order by year, coverage_type
