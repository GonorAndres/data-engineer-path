-- Atomic grain for the KPI layer: accident/exposure year x coverage x state.
-- Claims and exposure are aggregated separately, then full-outer joined, the
-- same way rpt_claim_frequency does it. Roll-ups in kpi_portfolio sum these
-- additive measures and apply the KPI formulas once, at the coarser grain.
with claims_agg as (
    select
        accident_year,
        coverage_type,
        state_code,
        count(*) as claim_count,
        sum(total_paid) as total_paid,
        sum(incurred_amount) as total_incurred
    from {{ ref('fct_claims') }}
    group by accident_year, coverage_type, state_code
),

exposure_agg as (
    select
        e.exposure_year,
        e.coverage_type,
        ph.state_code,
        count(*) as policy_count,
        sum(e.exposure_years) as exposure_years,
        sum(e.earned_premium) as earned_premium
    from {{ ref('int_policy_exposure') }} as e
    left join {{ ref('stg_policyholders') }} as ph on e.policyholder_id = ph.policyholder_id
    group by e.exposure_year, e.coverage_type, ph.state_code
)

select
    cast(coalesce(c.accident_year, e.exposure_year) as integer) as accident_year,
    coalesce(c.coverage_type, e.coverage_type) as coverage_type,
    coalesce(c.state_code, e.state_code) as state_code,
    coalesce(e.policy_count, 0) as policy_count,
    coalesce(c.claim_count, 0) as claim_count,
    coalesce(e.exposure_years, 0) as exposure_years,
    coalesce(e.earned_premium, 0) as earned_premium,
    coalesce(c.total_paid, 0) as total_paid,
    coalesce(c.total_incurred, 0) as total_incurred
from claims_agg as c
full outer join exposure_agg as e
    on
        c.accident_year = e.exposure_year
        and c.coverage_type = e.coverage_type
        and c.state_code = e.state_code
