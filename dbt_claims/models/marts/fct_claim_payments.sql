select
    p.payment_id,
    p.claim_id,
    {{ claims.date_key('p.payment_date') }} as payment_date_key,
    {{ claims.date_key('p.accident_date') }} as accident_date_key,
    p.payment_date,
    p.accident_date,
    p.development_year,
    p.development_months,
    p.accident_year,
    p.payment_amount,
    p.payment_type,
    p.cumulative_paid,
    p.payment_rank,
    p.coverage_type,
    p.claim_status
from {{ ref('int_claim_payments_cumulative') }} as p
