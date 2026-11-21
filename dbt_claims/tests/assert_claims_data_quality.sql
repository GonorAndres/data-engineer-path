-- Ported from Dataform assertion assert_claims_data_quality.
-- Returns rows that violate a rule; the test fails if any row comes back.
select
    claim_id,
    accident_date,
    report_date,
    claim_status,
    'report_before_accident' as violation
from {{ ref('fct_claims') }}
where report_date < accident_date

union all

select
    claim_id,
    accident_date,
    report_date,
    claim_status,
    'closed_with_reserve' as violation
from {{ ref('fct_claims') }}
where
    claim_status in ('closed', 'denied')
    and current_reserve != 0

union all

select
    claim_id,
    accident_date,
    report_date,
    claim_status,
    'negative_incurred' as violation
from {{ ref('fct_claims') }}
where incurred_amount < -10000
