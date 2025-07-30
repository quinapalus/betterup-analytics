{% macro contract_key(contract_id) -%}

{{ dbt_utils.surrogate_key([contract_id]) }}

{%- endmacro %}
