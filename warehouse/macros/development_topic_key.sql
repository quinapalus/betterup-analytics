{% macro development_topic_key(app_development_topic_id) -%}

{{ dbt_utils.surrogate_key([app_development_topic_id]) }}

{%- endmacro %}
