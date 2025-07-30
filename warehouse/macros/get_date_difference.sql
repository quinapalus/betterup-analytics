{% macro get_date_difference(first_date, second_date) -%}

-- DEPRECATED. USE get_day_difference() macro instead.

CEIL(DATEDIFF('second', {{ first_date }}, {{ second_date }}) / 86400)

{%- endmacro %}
