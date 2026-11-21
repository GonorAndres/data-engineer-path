{% test development_year_non_negative(model, column_name) %}
    {# Development year = payment year - accident year. A negative value means
       a payment dated before the accident year. #}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}
