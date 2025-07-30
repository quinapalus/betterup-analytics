{% macro deployment_key(app_track_id) -%}

{{ dbt_utils.surrogate_key([app_track_id]) }}

{%- endmacro %}
