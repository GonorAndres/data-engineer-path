-- The source data has only a state code per policyholder, no state names, so
-- this dimension carries just the distinct codes and a surrogate key.
select
    row_number() over (order by state_code) as state_key,
    state_code
from (
    select distinct state_code
    from {{ ref('stg_policyholders') }}
) as states
