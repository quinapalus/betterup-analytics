{% macro datespine(datepart, start_date, end_date) %}
    ({{dbt_utils.date_spine(datepart, start_date, end_date)}})
{% endmacro %}
