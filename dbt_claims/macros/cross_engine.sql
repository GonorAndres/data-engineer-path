{#
  Small helpers that keep model SQL identical on DuckDB and Snowflake.
  Most portability comes from dbt's own cross-database macros (dbt.datediff,
  dbt.safe_cast, dbt.type_float, dbt.date_spine). Only what dbt does not cover
  lives here.
#}

{# Money columns: fixed precision so both engines produce the same type.
   dbt.type_numeric() is not used because its precision differs by adapter. #}
{% macro type_money() %}decimal(18, 2){% endmacro %}

{# 64-bit float. dbt.type_float() is a 32-bit REAL on DuckDB, which loses
   precision on premium sums, so the type is spelled out. Both engines accept it. #}
{% macro type_double() %}double precision{% endmacro %}

{# Valuation date and observation-window start, from dbt_project.yml vars. #}
{% macro valuation_date() %}cast('{{ var("valuation_date") }}' as date){% endmacro %}

{% macro observation_start() %}cast('{{ var("observation_start") }}' as date){% endmacro %}

{# YYYYMMDD integer key. Built from extract() so no strftime/to_char is needed. #}
{% macro date_key(column) %}
    cast(
        extract(year from {{ column }}) * 10000
        + extract(month from {{ column }}) * 100
        + extract(day from {{ column }}) as integer
    )
{% endmacro %}

{# Day of week, 1 = Sunday ... 7 = Saturday (BigQuery DAYOFWEEK convention).
   ISO day (Mon = 1 ... Sun = 7) is used because it does not depend on any
   session setting such as Snowflake's WEEK_START. #}
{% macro day_of_week(column) %}({{ claims.iso_dow(column) }} % 7 + 1){% endmacro %}

{# ISO week number. The engines name this differently, so it dispatches. #}
{% macro iso_week(column) %}
    {{ return(adapter.dispatch('iso_week', 'claims')(column)) }}
{% endmacro %}

{% macro default__iso_week(column) %}extract(week from {{ column }}){% endmacro %}

{% macro duckdb__iso_week(column) %}extract(week from {{ column }}){% endmacro %}

{% macro snowflake__iso_week(column) %}weekiso({{ column }}){% endmacro %}

{# ISO day of week (Mon = 1 ... Sun = 7). DuckDB has isodow(), Snowflake has
   dayofweekiso(). Used by day_of_week() above. #}
{% macro iso_dow(column) %}
    {{ return(adapter.dispatch('iso_dow', 'claims')(column)) }}
{% endmacro %}

{% macro default__iso_dow(column) %}dayofweekiso({{ column }}){% endmacro %}

{% macro snowflake__iso_dow(column) %}dayofweekiso({{ column }}){% endmacro %}

{% macro duckdb__iso_dow(column) %}isodow({{ column }}){% endmacro %}

{# English day and month names via CASE: to_char/strftime format codes differ
   between engines and Snowflake has no full-weekday format. #}
{% macro day_name(day_of_week_expr) %}
    case {{ day_of_week_expr }}
        when 1 then 'Sunday' when 2 then 'Monday' when 3 then 'Tuesday'
        when 4 then 'Wednesday' when 5 then 'Thursday' when 6 then 'Friday'
        when 7 then 'Saturday'
    end
{% endmacro %}

{% macro month_name(month_expr) %}
    case {{ month_expr }}
        when 1 then 'January' when 2 then 'February' when 3 then 'March'
        when 4 then 'April' when 5 then 'May' when 6 then 'June'
        when 7 then 'July' when 8 then 'August' when 9 then 'September'
        when 10 then 'October' when 11 then 'November' when 12 then 'December'
    end
{% endmacro %}
