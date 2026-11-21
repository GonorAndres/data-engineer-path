select
    cast(payment_id as integer) as payment_id,
    cast(claim_id as integer) as claim_id,
    cast(payment_date as date) as payment_date,
    cast(payment_amount as {{ claims.type_money() }}) as payment_amount,
    lower(trim(payment_type)) as payment_type,
    cast(cumulative_paid as {{ claims.type_money() }}) as cumulative_paid
from {{ ref('raw_claim_payments') }}
where payment_id is not null
