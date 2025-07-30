{% macro get_json_object(json_string, path, object_type='string') -%}

parse_json({{ json_string }}):{{ path }}::{{ object_type }}

{%- endmacro %}
