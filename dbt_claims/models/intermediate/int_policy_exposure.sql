with exposure_calc as (
    select
        policy_id,
        policyholder_id,
        policy_number,
        coverage_type,
        effective_date,
        expiration_date,
        annual_premium,
        status,
        greatest(effective_date, {{ claims.observation_start() }}) as exposure_start,
        least(expiration_date, {{ claims.valuation_date() }}) as exposure_end
    from {{ ref('stg_policies') }}
    where status != 'cancelled'
),

with_days as (
    select
        *,
        {{ dbt.datediff('exposure_start', 'exposure_end', 'day') }} as exposure_days,
        {{ dbt.datediff('effective_date', 'expiration_date', 'day') }} as term_days
    from exposure_calc
)

select
    policy_id,
    policyholder_id,
    policy_number,
    coverage_type,
    effective_date,
    expiration_date,
    annual_premium,
    status,
    exposure_start,
    exposure_end,
    exposure_days,
    cast(exposure_days as {{ claims.type_double() }}) / 365.25 as exposure_years,
    round(
        annual_premium
        * cast(exposure_days as {{ claims.type_double() }})
        / cast(term_days as {{ claims.type_double() }}),
        2
    ) as earned_premium,
    extract(year from greatest(effective_date, {{ claims.observation_start() }})) as exposure_year
from with_days
where exposure_end > exposure_start
