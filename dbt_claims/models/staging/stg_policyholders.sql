select
    cast(policyholder_id as integer) as policyholder_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    cast(date_of_birth as date) as date_of_birth,
    upper(trim(gender)) as gender,
    upper(trim(state_code)) as state_code,
    trim(city) as city,
    lower(trim(occupation)) as occupation,
    cast(registration_date as date) as registration_date
from {{ ref('raw_policyholders') }}
where policyholder_id is not null
