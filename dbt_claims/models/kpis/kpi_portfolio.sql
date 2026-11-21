with rolled_up as (
    {{ claims.kpi_rollup('overall', []) }}
    union all
    {{ claims.kpi_rollup('coverage_type', ['coverage_type']) }}
    union all
    {{ claims.kpi_rollup('accident_year', ['accident_year']) }}
    union all
    {{ claims.kpi_rollup('state_code', ['state_code']) }}
)

select
    grain,
    accident_year,
    coverage_type,
    state_code,
    policy_count,
    claim_count,
    exposure_years,
    earned_premium,
    total_paid,
    total_incurred,
    round({{ claims.kpi_frequency('claim_count', 'exposure_years') }}, 4) as claim_frequency,
    round({{ claims.kpi_severity('total_paid', 'claim_count') }}, 2) as severity,
    round({{ claims.kpi_pure_premium('total_incurred', 'exposure_years') }}, 2) as pure_premium,
    round({{ claims.kpi_loss_ratio('total_incurred', 'earned_premium') }}, 4) as loss_ratio
from rolled_up
