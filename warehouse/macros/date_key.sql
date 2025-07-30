{% macro date_key(ts) -%}

CASE WHEN {{ ts }} IS NOT NULL THEN to_char({{ ts }}, 'YYYYMMDD')::int END

{%- endmacro %}
