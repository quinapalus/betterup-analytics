{% macro is_current_date(date_key) -%}

{{ date_key }} = TO_CHAR(CURRENT_DATE, 'YYYYMMDD')::INT

{%- endmacro %}
