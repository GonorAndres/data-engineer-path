-- Reconciliation: row counts per model and sums of key money columns.
-- Run on DuckDB:  dbt compile --select reconcile_bigquery  (then run target/compiled SQL)
--             or  dbt show --inline "$(cat analyses/reconcile_bigquery.sql)" ... (simplest: copy the compiled SQL).
-- Run the same numbers on BigQuery with the equivalent queries in the comment
-- block at the bottom (dev_claims_* datasets, as Dataform builds them).
-- Models that exist only in dbt (no BigQuery counterpart): dim_state,
-- int_kpi_base, kpi_portfolio, snap_policies.
-- Floating-point sums (earned_premium, exposure_years) can differ in the last
-- digits between engines; compare them rounded to 2 decimals.

select 'stg_claims' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('stg_claims') }}
union all
select 'stg_claim_payments' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('stg_claim_payments') }}
union all
select 'stg_coverages' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('stg_coverages') }}
union all
select 'stg_policies' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('stg_policies') }}
union all
select 'stg_policyholders' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('stg_policyholders') }}
union all
select 'int_claim_payments_cumulative' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('int_claim_payments_cumulative') }}
union all
select 'int_claims_enriched' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('int_claims_enriched') }}
union all
select 'int_policy_exposure' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('int_policy_exposure') }}
union all
select 'dim_coverage' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('dim_coverage') }}
union all
select 'dim_date' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('dim_date') }}
union all
select 'dim_policyholder' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('dim_policyholder') }}
union all
select 'dim_policy' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('dim_policy') }}
union all
select 'fct_claims' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('fct_claims') }}
union all
select 'fct_claim_payments' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('fct_claim_payments') }}
union all
select 'rpt_claim_frequency' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('rpt_claim_frequency') }}
union all
select 'rpt_loss_triangle' as model, 'row_count' as metric, cast(count(*) as {{ claims.type_double() }}) as value from {{ ref('rpt_loss_triangle') }}
union all
select 'fct_claims' as model, 'sum_total_paid' as metric, cast(sum(total_paid) as {{ claims.type_double() }}) as value from {{ ref('fct_claims') }}
union all
select 'fct_claims' as model, 'sum_total_indemnity' as metric, cast(sum(total_indemnity) as {{ claims.type_double() }}) as value from {{ ref('fct_claims') }}
union all
select 'fct_claims' as model, 'sum_total_expense' as metric, cast(sum(total_expense) as {{ claims.type_double() }}) as value from {{ ref('fct_claims') }}
union all
select 'fct_claims' as model, 'sum_total_recovery' as metric, cast(sum(total_recovery) as {{ claims.type_double() }}) as value from {{ ref('fct_claims') }}
union all
select 'fct_claims' as model, 'sum_current_reserve' as metric, cast(sum(current_reserve) as {{ claims.type_double() }}) as value from {{ ref('fct_claims') }}
union all
select 'fct_claims' as model, 'sum_incurred_amount' as metric, cast(sum(incurred_amount) as {{ claims.type_double() }}) as value from {{ ref('fct_claims') }}
union all
select 'fct_claim_payments' as model, 'sum_payment_amount' as metric, cast(sum(payment_amount) as {{ claims.type_double() }}) as value from {{ ref('fct_claim_payments') }}
union all
select 'dim_policy' as model, 'sum_annual_premium' as metric, cast(sum(annual_premium) as {{ claims.type_double() }}) as value from {{ ref('dim_policy') }}
union all
select 'int_policy_exposure' as model, 'sum_earned_premium' as metric, cast(sum(earned_premium) as {{ claims.type_double() }}) as value from {{ ref('int_policy_exposure') }}
union all
select 'int_policy_exposure' as model, 'sum_exposure_years' as metric, cast(sum(exposure_years) as {{ claims.type_double() }}) as value from {{ ref('int_policy_exposure') }}
union all
select 'rpt_claim_frequency' as model, 'sum_claim_count' as metric, cast(sum(claim_count) as {{ claims.type_double() }}) as value from {{ ref('rpt_claim_frequency') }}
union all
select 'rpt_claim_frequency' as model, 'sum_total_incurred' as metric, cast(sum(total_incurred) as {{ claims.type_double() }}) as value from {{ ref('rpt_claim_frequency') }}
order by model, metric

/* BigQuery equivalents (replace `PROJECT`):

select 'stg_claims' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_staging.stg_claims`
union all
select 'stg_claim_payments' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_staging.stg_claim_payments`
union all
select 'stg_coverages' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_staging.stg_coverages`
union all
select 'stg_policies' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_staging.stg_policies`
union all
select 'stg_policyholders' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_staging.stg_policyholders`
union all
select 'int_claim_payments_cumulative' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_intermediate.int_claim_payments_cumulative`
union all
select 'int_claims_enriched' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_intermediate.int_claims_enriched`
union all
select 'int_policy_exposure' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_intermediate.int_policy_exposure`
union all
select 'dim_coverage' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_analytics.dim_coverage`
union all
select 'dim_date' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_analytics.dim_date`
union all
select 'dim_policyholder' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_analytics.dim_policyholder`
union all
select 'dim_policy' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_analytics.dim_policy`
union all
select 'fct_claims' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_analytics.fct_claims`
union all
select 'fct_claim_payments' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_analytics.fct_claim_payments`
union all
select 'rpt_claim_frequency' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_reports.rpt_claim_frequency`
union all
select 'rpt_loss_triangle' as model, 'row_count' as metric, count(*) as value from `PROJECT.dev_claims_reports.rpt_loss_triangle`
union all
select 'fct_claims', 'sum_total_paid', cast(sum(total_paid) as float64) from `PROJECT.dev_claims_analytics.fct_claims`
union all
select 'fct_claims', 'sum_total_indemnity', cast(sum(total_indemnity) as float64) from `PROJECT.dev_claims_analytics.fct_claims`
union all
select 'fct_claims', 'sum_total_expense', cast(sum(total_expense) as float64) from `PROJECT.dev_claims_analytics.fct_claims`
union all
select 'fct_claims', 'sum_total_recovery', cast(sum(total_recovery) as float64) from `PROJECT.dev_claims_analytics.fct_claims`
union all
select 'fct_claims', 'sum_current_reserve', cast(sum(current_reserve) as float64) from `PROJECT.dev_claims_analytics.fct_claims`
union all
select 'fct_claims', 'sum_incurred_amount', cast(sum(incurred_amount) as float64) from `PROJECT.dev_claims_analytics.fct_claims`
union all
select 'fct_claim_payments', 'sum_payment_amount', cast(sum(payment_amount) as float64) from `PROJECT.dev_claims_analytics.fct_claim_payments`
union all
select 'dim_policy', 'sum_annual_premium', cast(sum(annual_premium) as float64) from `PROJECT.dev_claims_analytics.dim_policy`
union all
select 'int_policy_exposure', 'sum_earned_premium', cast(sum(earned_premium) as float64) from `PROJECT.dev_claims_intermediate.int_policy_exposure`
union all
select 'int_policy_exposure', 'sum_exposure_years', cast(sum(exposure_years) as float64) from `PROJECT.dev_claims_intermediate.int_policy_exposure`
union all
select 'rpt_claim_frequency', 'sum_claim_count', cast(sum(claim_count) as float64) from `PROJECT.dev_claims_reports.rpt_claim_frequency`
union all
select 'rpt_claim_frequency', 'sum_total_incurred', cast(sum(total_incurred) as float64) from `PROJECT.dev_claims_reports.rpt_claim_frequency`
order by 1, 2;
*/
