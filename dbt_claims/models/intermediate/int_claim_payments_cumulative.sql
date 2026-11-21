with claim_context as (
    select
        sc.claim_id,
        sc.accident_date,
        sc.accident_year,
        sp.coverage_type,
        sc.claim_status
    from {{ ref('stg_claims') }} as sc
    inner join {{ ref('stg_policies') }} as sp on sc.policy_id = sp.policy_id
),

payments_with_claim as (
    select
        p.payment_id,
        p.claim_id,
        p.payment_date,
        p.payment_amount,
        p.payment_type,
        c.accident_date,
        c.accident_year,
        c.coverage_type,
        c.claim_status
    from {{ ref('stg_claim_payments') }} as p
    inner join claim_context as c on p.claim_id = c.claim_id
)

select
    payment_id,
    claim_id,
    payment_date,
    payment_amount,
    payment_type,
    accident_date,
    accident_year,
    coverage_type,
    claim_status,
    extract(year from payment_date) - accident_year as development_year,
    (extract(year from payment_date) - extract(year from accident_date)) * 12
    + (extract(month from payment_date) - extract(month from accident_date))
        as development_months,
    sum(payment_amount) over (
        partition by claim_id
        order by payment_date, payment_id
    ) as cumulative_paid,
    row_number() over (
        partition by claim_id
        order by payment_date, payment_id
    ) as payment_rank
from payments_with_claim
