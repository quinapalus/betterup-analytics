{% macro snake_case(string) -%}

lower(replace({{ string }}, ' ', '_'))

{%- endmacro %}
