{% macro is_current_month(month_key) -%}

-- used in incremental models in which current day is surfaced only after midnight.

{{ month_key }} = TO_CHAR(CURRENT_DATE - INTERVAL '1 DAY', 'YYYYMM')::INT

{%- endmacro %}
