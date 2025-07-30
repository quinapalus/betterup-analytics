{% macro coach_key(fnt_applicant_id, app_coach_id) -%}

{{ dbt_utils.surrogate_key([fnt_applicant_id, app_coach_id]) }}

{%- endmacro %}
