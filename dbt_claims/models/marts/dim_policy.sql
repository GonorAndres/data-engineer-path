select
    p.policy_id,
    p.policyholder_id,
    p.policy_number,
    p.coverage_type,
    c.coverage_category,
    c.description as coverage_description,
    p.effective_date,
    p.expiration_date,
    p.policy_term_days,
    p.annual_premium,
    p.deductible,
    p.coverage_limit,
    p.status,
    extract(year from p.effective_date) as policy_year,
    (
        p.status = 'active'
        and p.effective_date <= {{ claims.valuation_date() }}
        and p.expiration_date > {{ claims.valuation_date() }}
    ) as is_currently_active
from {{ ref('stg_policies') }} as p
left join {{ ref('stg_coverages') }} as c on p.coverage_type = c.coverage_type
