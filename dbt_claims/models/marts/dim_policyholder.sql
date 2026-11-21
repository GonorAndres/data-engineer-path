-- The policyholder is the insured party in this warehouse.
select
    policyholder_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    date_of_birth,
    {{ dbt.datediff('date_of_birth', 'registration_date', 'year') }} as age_at_registration,
    {{ dbt.datediff('date_of_birth', claims.valuation_date(), 'year') }} as current_age,
    gender,
    state_code,
    city,
    occupation,
    registration_date
from {{ ref('stg_policyholders') }}
