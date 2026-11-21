-- Additive change vs Dataform: policyholder state_code is joined in so the
-- claims fact can reference dim_state.
with claim_policy as (
    select
        c.claim_id,
        c.policy_id,
        c.claim_number,
        c.accident_date,
        c.report_date,
        c.close_date,
        c.claim_status,
        c.cause_of_loss,
        c.initial_reserve,
        c.current_reserve,
        c.report_delay_days,
        c.accident_year,
        c.accident_quarter,
        p.policyholder_id,
        p.policy_number,
        p.coverage_type,
        p.effective_date as policy_effective_date,
        p.expiration_date as policy_expiration_date,
        p.annual_premium,
        p.deductible,
        p.coverage_limit,
        p.status as policy_status,
        cov.coverage_category,
        cov.description as coverage_description,
        ph.state_code
    from {{ ref('stg_claims') }} as c
    inner join {{ ref('stg_policies') }} as p on c.policy_id = p.policy_id
    left join {{ ref('stg_coverages') }} as cov on p.coverage_type = cov.coverage_type
    left join {{ ref('stg_policyholders') }} as ph on p.policyholder_id = ph.policyholder_id
)

select
    *,
    (extract(year from {{ claims.valuation_date() }}) - extract(year from accident_date)) * 12
    + (extract(month from {{ claims.valuation_date() }}) - extract(month from accident_date))
        as development_months_at_valuation,
    coalesce(initial_reserve > 200000, false) as is_large_loss
from claim_policy
