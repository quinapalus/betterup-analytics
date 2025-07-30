{% macro session_key(app_session_id, app_member_id, app_coach_id, app_starts_at) -%}

{{ dbt_utils.surrogate_key([app_session_id, app_member_id, app_coach_id, app_starts_at]) }}

{%- endmacro %}
