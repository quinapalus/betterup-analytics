{% macro is_latest_complete_month(month_key) -%}

{{ month_key }} = TO_CHAR(CURRENT_DATE - INTERVAL '1 MONTH', 'YYYYMM')::INT

{%- endmacro %}
