{% macro month_key(ts) -%}

to_char({{ ts }}, 'YYYYMM')::int

{%- endmacro %}
