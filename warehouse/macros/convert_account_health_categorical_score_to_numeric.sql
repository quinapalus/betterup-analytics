{% macro convert_account_health_categorical_score_to_numeric(column_name) %}

case
    when {{ column_name }} = 'Good'
        then 10
    when {{ column_name }} = 'Okay'
        then 5
    when {{ column_name }} = 'Poor'
        then 1
    when {{ column_name }} = 'Not Enough Data' or {{ column_name }} is null
        then 0 end

{%- endmacro %}
