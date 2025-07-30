{% macro member_key(member_id) -%}

{{ dbt_utils.surrogate_key([member_id]) }}

{%- endmacro %}
