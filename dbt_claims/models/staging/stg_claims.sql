select
    cast(claim_id as integer) as claim_id,
    cast(policy_id as integer) as policy_id,
    trim(claim_number) as claim_number,
    cast(accident_date as date) as accident_date,
    cast(report_date as date) as report_date,
    {{ dbt.safe_cast('close_date', 'date') }} as close_date,
    lower(trim(claim_status)) as claim_status,
    lower(trim(cause_of_loss)) as cause_of_loss,
    cast(initial_reserve as {{ claims.type_money() }}) as initial_reserve,
    cast(current_reserve as {{ claims.type_money() }}) as current_reserve,
    {{ dbt.datediff('cast(accident_date as date)', 'cast(report_date as date)', 'day') }} as report_delay_days,
    extract(year from cast(accident_date as date)) as accident_year,
    extract(quarter from cast(accident_date as date)) as accident_quarter
from {{ ref('raw_claims') }}
where claim_id is not null
