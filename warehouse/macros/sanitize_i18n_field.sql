{% macro sanitize_i18n_field(field) -%}

{{ field }}_i18n,
COALESCE(parse_json({{ field }}_i18n):"en"::varchar,
         parse_json({{ field }}_i18n):"en-us"::varchar,
         parse_json({{ field }}_i18n):"en-US"::varchar) AS {{ field }},

{%- endmacro %}
