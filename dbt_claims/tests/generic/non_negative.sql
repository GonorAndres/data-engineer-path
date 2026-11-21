{% test non_negative(model, column_name) %}
    {# Fails for every row where the column is below zero. Nulls pass. #}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}
