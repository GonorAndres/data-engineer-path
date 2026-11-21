select
    cast(policy_id as integer) as policy_id,
    cast(policyholder_id as integer) as policyholder_id,
    trim(policy_number) as policy_number,
    lower(trim(coverage_type)) as coverage_type,
    cast(effective_date as date) as effective_date,
    cast(expiration_date as date) as expiration_date,
    cast(annual_premium as {{ claims.type_money() }}) as annual_premium,
    cast(deductible as {{ claims.type_money() }}) as deductible,
    cast(coverage_limit as {{ claims.type_money() }}) as coverage_limit,
    lower(trim(status)) as status,
    {{ dbt.datediff('cast(effective_date as date)', 'cast(expiration_date as date)', 'day') }} as policy_term_days
from {{ ref('raw_policies') }}
where
    policy_id is not null
    and cast(effective_date as date) < cast(expiration_date as date)
