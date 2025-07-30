{% macro member_deployment_key(app_member_id, app_track_id) -%}

{{ dbt_utils.surrogate_key([app_member_id, app_track_id]) }}

{%- endmacro %}
