{#
  KPI definitions. Each formula is written once, here, and reused by
  rpt_claim_frequency and kpi_portfolio. The definitions are the ones Dataform's
  rpt_claim_frequency uses:

    frequency     = claim_count   / exposure_years
    severity      = total_paid    / claim_count
    pure_premium  = total_incurred / exposure_years
    loss_ratio    = total_incurred / earned_premium

  A zero or null denominator returns null. Rounding is left to the caller.
#}

{% macro safe_ratio(numerator, denominator) %}
    case
        when coalesce({{ denominator }}, 0) > 0
            then cast({{ numerator }} as {{ claims.type_double() }}) / {{ denominator }}
    end
{% endmacro %}

{% macro kpi_frequency(claim_count, exposure_years) %}{{ claims.safe_ratio(claim_count, exposure_years) }}{% endmacro %}

{% macro kpi_severity(total_paid, claim_count) %}{{ claims.safe_ratio(total_paid, claim_count) }}{% endmacro %}

{% macro kpi_pure_premium(total_incurred, exposure_years) %}{{ claims.safe_ratio(total_incurred, exposure_years) }}{% endmacro %}

{% macro kpi_loss_ratio(total_incurred, earned_premium) %}{{ claims.safe_ratio(total_incurred, earned_premium) }}{% endmacro %}

{#
  One roll-up of int_kpi_base to the requested grain. `dims` lists the columns
  to group by; every other dimension column is returned as null so all grains
  share one schema and can be unioned.
#}
{% macro kpi_rollup(grain, dims) %}
    select
        '{{ grain }}' as grain,
        {% if 'accident_year' in dims %}accident_year{% else %}cast(null as integer){% endif %} as accident_year,
        {% if 'coverage_type' in dims %}coverage_type{% else %}cast(null as varchar){% endif %} as coverage_type,
        {% if 'state_code' in dims %}state_code{% else %}cast(null as varchar){% endif %} as state_code,
        sum(policy_count) as policy_count,
        sum(claim_count) as claim_count,
        sum(exposure_years) as exposure_years,
        sum(earned_premium) as earned_premium,
        sum(total_paid) as total_paid,
        sum(total_incurred) as total_incurred
    from {{ ref('int_kpi_base') }}
    {% if dims %}group by {{ dims | join(', ') }}{% endif %}
{% endmacro %}
