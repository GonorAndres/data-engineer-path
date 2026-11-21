with claim_payments_summary as (
    select
        claim_id,
        sum(case when payment_type = 'indemnity' then payment_amount else 0 end) as total_indemnity,
        sum(case when payment_type = 'expense' then payment_amount else 0 end) as total_expense,
        sum(case when payment_type = 'recovery' then payment_amount else 0 end) as total_recovery,
        sum(payment_amount) as total_paid,
        count(*) as payment_count,
        min(payment_date) as first_payment_date,
        max(payment_date) as last_payment_date
    from {{ ref('stg_claim_payments') }}
    group by claim_id
)

select
    c.claim_id,
    c.policy_id,
    c.policyholder_id,
    c.claim_number,
    {{ claims.date_key('c.accident_date') }} as accident_date_key,
    {{ claims.date_key('c.report_date') }} as report_date_key,
    c.accident_date,
    c.report_date,
    c.close_date,
    c.claim_status,
    c.cause_of_loss,
    c.coverage_type,
    c.coverage_category,
    c.state_code,
    c.accident_year,
    c.accident_quarter,
    c.report_delay_days,
    c.development_months_at_valuation,
    c.is_large_loss,
    c.annual_premium,
    c.deductible,
    c.coverage_limit,
    c.initial_reserve,
    c.current_reserve,
    coalesce(ps.total_indemnity, 0) as total_indemnity,
    coalesce(ps.total_expense, 0) as total_expense,
    coalesce(ps.total_recovery, 0) as total_recovery,
    coalesce(ps.total_paid, 0) as total_paid,
    coalesce(ps.payment_count, 0) as payment_count,
    ps.first_payment_date,
    ps.last_payment_date,
    coalesce(ps.total_paid, 0) + c.current_reserve as incurred_amount,
    coalesce(ps.total_paid, 0) as net_paid
from {{ ref('int_claims_enriched') }} as c
left join claim_payments_summary as ps on c.claim_id = ps.claim_id
