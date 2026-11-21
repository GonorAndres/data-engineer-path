-- Ported from Dataform assertion assert_referential_integrity.
-- Orphan claims: no matching policy.
select
    c.claim_id,
    c.policy_id,
    'orphan_claim' as violation
from {{ ref('fct_claims') }} as c
left join {{ ref('dim_policy') }} as p on c.policy_id = p.policy_id
where p.policy_id is null

union all

-- Orphan payments: no matching claim. (Dataform reuses claim_id/policy_id
-- columns to carry payment_id/claim_id; kept the same here.)
select
    fp.payment_id as claim_id,
    fp.claim_id as policy_id,
    'orphan_payment' as violation
from {{ ref('fct_claim_payments') }} as fp
left join {{ ref('fct_claims') }} as c on fp.claim_id = c.claim_id
where c.claim_id is null
