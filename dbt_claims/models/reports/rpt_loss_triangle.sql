with incremental_by_dev as (
    select
        accident_year,
        development_year,
        sum(payment_amount) as incremental_paid
    from {{ ref('fct_claim_payments') }}
    where development_year >= 0
    group by accident_year, development_year
),

cumulative_by_dev as (
    select
        accident_year,
        development_year,
        incremental_paid,
        sum(incremental_paid) over (
            partition by accident_year
            order by development_year
        ) as cumulative_paid
    from incremental_by_dev
),

claim_counts as (
    select
        accident_year,
        count(distinct claim_id) as claim_count
    from {{ ref('fct_claim_payments') }}
    group by accident_year
)

select
    cum.accident_year,
    max(case when cum.development_year = 0 then cum.cumulative_paid end) as dev_year_0,
    max(case when cum.development_year = 1 then cum.cumulative_paid end) as dev_year_1,
    max(case when cum.development_year = 2 then cum.cumulative_paid end) as dev_year_2,
    max(case when cum.development_year = 3 then cum.cumulative_paid end) as dev_year_3,
    max(case when cum.development_year = 4 then cum.cumulative_paid end) as dev_year_4,
    max(case when cum.development_year = 5 then cum.cumulative_paid end) as dev_year_5,
    max(cum.cumulative_paid) as latest_cumulative,
    max(cc.claim_count) as claim_count
from cumulative_by_dev as cum
left join claim_counts as cc on cum.accident_year = cc.accident_year
group by cum.accident_year
order by cum.accident_year
