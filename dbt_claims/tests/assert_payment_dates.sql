-- Ported from Dataform assertion assert_payment_dates.
select
    payment_id,
    claim_id,
    payment_date,
    accident_date,
    'payment_before_accident' as violation
from {{ ref('fct_claim_payments') }}
where payment_date < accident_date
