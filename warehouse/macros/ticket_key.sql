{% macro ticket_key(ticket_id) -%}

{{ dbt_utils.surrogate_key([ticket_id]) }}

{%- endmacro %}
