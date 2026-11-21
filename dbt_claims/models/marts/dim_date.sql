with date_spine as (
    {{ dbt.date_spine(
        datepart='day',
        start_date="cast('2019-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
),

days as (
    select
        cast(date_day as date) as full_date,
        {{ claims.day_of_week('cast(date_day as date)') }} as day_of_week
    from date_spine
)

select
    {{ claims.date_key('full_date') }} as date_key,
    full_date,
    extract(year from full_date) as year,
    extract(quarter from full_date) as quarter,
    extract(month from full_date) as month,
    extract(day from full_date) as day,
    day_of_week,
    {{ claims.day_name('day_of_week') }} as day_name,
    {{ claims.month_name('extract(month from full_date)') }} as month_name,
    {{ claims.iso_week('full_date') }} as iso_week,
    coalesce(day_of_week in (1, 7), false) as is_weekend,
    full_date = last_day(full_date) as is_month_end,
    (extract(month from full_date) in (3, 6, 9, 12) and full_date = last_day(full_date)) as is_quarter_end,
    (extract(month from full_date) = 12 and extract(day from full_date) = 31) as is_year_end,
    extract(year from full_date) as fiscal_year,
    extract(quarter from full_date) as fiscal_quarter,
    cast(extract(year from full_date) as varchar) || '-Q' || cast(extract(quarter from full_date) as varchar) as year_quarter_label
from days
